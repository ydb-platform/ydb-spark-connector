package tech.ydb.spark.connector.write;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import tech.ydb.core.Status;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.FieldType;
import tech.ydb.spark.connector.common.IngestMethod;
import tech.ydb.spark.connector.impl.YdbConnector;
import tech.ydb.spark.connector.impl.YdbTypes;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB table writer: basic row writer.
 *
 * @author zinal
 */
public class YdbWriterImpl implements DataWriter<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(YdbWriterImpl.class);

    private final int partitionId;
    private final long taskId;

    private final List<StructField> inputFields;
    private final List<FieldInfo> statementFields;
    private final String sqlStatement;
    private final String tablePath;
    private final IngestMethod ingestMethod;

    private final tech.ydb.table.values.StructType inputType;
    private final tech.ydb.table.values.ListType listType;
    private final int maxBulkRows;

    private final YdbConnector connector;
    private final YdbTypes types;
    private final List<Value<?>> currentInput;
    private CompletableFuture<Status> currentStatus;

    public YdbWriterImpl(YdbWriteOptions options, int partitionId, long taskId) {
        this.connector = options.getTable().getConnector();
        this.types = options.getTable().getTypes();
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.inputFields = new ArrayList<>(JavaConverters.asJavaCollection(options.getInputType().toList()));
        this.statementFields = makeStatementFields(options.getTable().getTypes(), options, this.inputFields);
        this.sqlStatement = makeSql(options, this.statementFields);
        this.tablePath = options.getTable().getTablePath();
        this.inputType = makeInputType(this.statementFields);
        this.listType = tech.ydb.table.values.ListType.of(this.inputType);
        this.ingestMethod = options.getIngestMethod();
        this.maxBulkRows = options.getMaxBatchSize();
        this.currentInput = new ArrayList<>();
        this.currentStatus = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("[{}, {}] YQL statement: {}", partitionId, taskId, this.sqlStatement);
            LOG.debug("[{}, {}] Input structure: {}", partitionId, taskId, this.inputType);
        }
    }

    @Override
    public void write(InternalRow record) throws IOException {
        currentInput.add(StructValue.of(convertRow(record)));
        if (currentInput.size() >= maxBulkRows) {
            startNewStatement(currentInput);
            currentInput.clear();
        }
    }

    private Map<String, Value<?>> convertRow(InternalRow record) throws IOException {
        final int numFields = record.numFields();
        if (numFields != inputFields.size()) {
            throw new IOException("Incorrect input record field count, expected "
                    + Integer.toString(inputFields.size()) + ", actual "
                    + Integer.toString(record.numFields()));
        }
        Map<String, Value<?>> currentRow = new HashMap<>();
        for (int i = 0; i < numFields; ++i) {
            FieldInfo yfi = statementFields.get(i);
            final Object value = record.get(i, inputFields.get(i).dataType());
            currentRow.put(yfi.getName(), convertValue(value, yfi));
        }
        if (statementFields.size() > numFields) {
            // The last field should be the auto-generated PK.
            FieldInfo yfi = statementFields.get(numFields);
            currentRow.put(yfi.getName(), convertValue(randomPk(), yfi));
        }
        // LOG.debug("Converted input row: {}", currentRow);
        return currentRow;
    }

    private Value<?> convertValue(Object value, FieldInfo yfi) {
        Value<?> conv = types.convertToYdb(value, yfi.getType());
        if (yfi.isNullable()) {
            if (conv.getType().getKind() != Type.Kind.OPTIONAL) {
                conv = conv.makeOptional();
            }
        }
        return conv;
    }

    private String randomPk() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());
    }

    private void startNewStatement(List<Value<?>> input) {
        LOG.debug("[{}, {}] Sending a batch of {} rows into table {}",
                partitionId, taskId, input.size(), tablePath);
        if (currentStatus != null) {
            currentStatus.join().expectSuccess();
            currentStatus = null;
            LOG.debug("[{}, {}] Previous async batch completed for table {}",
                    partitionId, taskId, tablePath);
        }
        // The list is being copied here.
        // DO NOT move this call into the async methods below.
        ListValue value = listType.newValue(input);
        if (IngestMethod.BULK_UPSERT.equals(ingestMethod)) {
            currentStatus = connector.getRetryCtx().supplyStatus(
                    session -> session.executeBulkUpsert(
                            tablePath, value, new BulkUpsertSettings()));
            LOG.debug("[{}, {}] Async bulk upsert started on table {}",
                    partitionId, taskId, tablePath);
        } else {
            currentStatus = connector.getRetryCtx().supplyStatus(
                    session -> session.executeDataQuery(sqlStatement,
                            TxControl.serializableRw().setCommitTx(true),
                            Params.of("$input", value))
                            .thenApply(result -> result.getStatus()));
            LOG.debug("[{}, {}] Async upsert transaction started on table {}",
                    partitionId, taskId, tablePath);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // Process the currently prepared set of rows
        if (!currentInput.isEmpty()) {
            startNewStatement(currentInput);
            currentInput.clear();
        }
        // Wait for the statement to be executed, and check the results
        if (currentStatus != null) {
            currentStatus.join().expectSuccess();
            currentStatus = null;
            LOG.debug("[{}, {}] Final async batch completed for table {}",
                    partitionId, taskId, tablePath);
        } else {
            LOG.debug("[{}, {}] Empty commit call for table {}",
                    partitionId, taskId, tablePath);
        }
        // All rows have been written successfully
        return new YdbWriteCommit();
    }

    @Override
    public void abort() throws IOException {
        LOG.debug("[{}, {}] Aborting writes for table {}", partitionId, taskId, tablePath);
        currentInput.clear();
        if (currentStatus != null) {
            currentStatus.cancel(true);
            currentStatus.join();
            currentStatus = null;
        }
    }

    @Override
    public void close() throws IOException {
        LOG.debug("[{}, {}] Closing the writer for table {}", partitionId, taskId, tablePath);
        currentInput.clear();
        if (currentStatus != null) {
            currentStatus.cancel(true);
            currentStatus.join();
            currentStatus = null;
        }
    }

    private static List<FieldInfo> makeStatementFields(YdbTypes types, YdbWriteOptions options,
            List<StructField> inputFields) {
        final List<FieldInfo> out = new ArrayList<>(inputFields.size());
//        if (options.isMapByNames()) {
//            for (StructField sf : inputFields) {
//                final String name = sf.name();
//                FieldInfo yfi = options.getFieldsMap().get(name);
//                if (yfi != null) {
//                    out.add(yfi);
//                }
//            }
//        } else {
            for (int pos = 0; pos < inputFields.size(); ++pos) {
                out.add(FieldInfo.fromSchema(types, inputFields.get(pos)));
            }
//        }
        if (options.getAddtitionalPk() != null) {
            // Generated PK is the last column, if one is presented at all.
            out.add(new FieldInfo(options.getAddtitionalPk(), FieldType.Text, false));
        }
        return out;
    }

    private static String makeSql(YdbWriteOptions options, List<FieldInfo> fields) {
        final StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $input AS List<Struct<");
        boolean comma = false;
        for (FieldInfo f : fields) {
            if (comma) {
                sb.append(", ");
            } else {
                comma = true;
            }
            sb.append("`").append(f.getName()).append("`");
            sb.append(": ");
            sb.append(f.getType().getSqlName());
            if (f.isNullable()) {
                sb.append("?");
            }
        }
        sb.append(">>;\n");
        switch (options.getIngestMethod()) {
            case BULK_UPSERT:
            /* unused sql statement, so use UPSERT to generate it */
            case UPSERT:
                sb.append("UPSERT INTO ");
                break;
            case REPLACE:
                sb.append("REPLACE INTO ");
                break;
            default: // unreached
                throw new UnsupportedOperationException();
        }
        sb.append("`").append(escape(options.getTable().getTablePath())).append("`");
        sb.append(" SELECT * FROM AS_TABLE($input);");
        return sb.toString();
    }

    public static String escape(String id) {
        if (id.contains("`")) {
            id = id.replace("`", "\\`");
        }
        if (id.contains("\n")) {
            id = id.replace("\n", "\\n");
        }
        if (id.contains("\r")) {
            id = id.replace("\r", "\\r");
        }
        return id;
    }

    private static StructType makeInputType(List<FieldInfo> statementFields) {
        if (statementFields.isEmpty()) {
            throw new IllegalArgumentException("Empty input field list specified for writing");
        }
        final Map<String, Type> m = new HashMap<>();
        for (FieldInfo yfi : statementFields) {
            m.put(yfi.getName(), FieldType.toSdkType(yfi.getType(), yfi.isNullable()));
        }
        return StructType.of(m);
    }

}
