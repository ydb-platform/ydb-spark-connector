package tech.ydb.spark.connector.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;

import tech.ydb.core.Status;
import tech.ydb.spark.connector.YdbFieldInfo;
import tech.ydb.spark.connector.YdbFieldType;
import tech.ydb.spark.connector.YdbIngestMethod;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.YdbWriteCommit;
import tech.ydb.spark.connector.YdbWriteOptions;
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

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbWriterImpl.class);

    private final int partitionId;
    private final long taskId;

    private final YdbTypes types;
    private final List<StructField> inputFields;
    private final List<YdbFieldInfo> statementFields;
    private final String sqlStatement;
    private final String tablePath;
    private final tech.ydb.table.values.StructType inputType;
    private final tech.ydb.table.values.ListType listType;
    private final YdbIngestMethod ingestMethod;
    private final int maxBulkRows;

    private final YdbConnector connector;
    private final List<Value<?>> currentInput;
    private CompletableFuture<Status> currentStatus;

    public YdbWriterImpl(YdbWriteOptions options, int partitionId, long taskId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.types = options.getTypes();
        this.inputFields = new ArrayList<>(JavaConverters.asJavaCollection(
                options.getInputType().toList()));
        this.statementFields = makeStatementFields(options, this.inputFields);
        this.sqlStatement = makeSql(options, this.statementFields);
        this.tablePath = options.getTablePath();
        this.inputType = makeInputType(this.statementFields);
        this.listType = tech.ydb.table.values.ListType.of(this.inputType);
        this.ingestMethod = options.getIngestMethod();
        this.maxBulkRows = options.getMaxBulkRows();
        this.connector = options.grabConnector();
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
            YdbFieldInfo yfi = statementFields.get(i);
            final Object value = record.get(i, inputFields.get(i).dataType());
            Value<?> conv = types.convertToYdb(value, yfi.getType());
            if (yfi.isNullable()) {
                if (conv.getType().getKind() != Type.Kind.OPTIONAL) {
                    conv = conv.makeOptional();
                }
            }
            currentRow.put(yfi.getName(), conv);
        }
        // LOG.debug("Converted input row: {}", currentRow);
        return currentRow;
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
        if (YdbIngestMethod.BULK.equals(ingestMethod)) {
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

    private static List<YdbFieldInfo> makeStatementFields(YdbWriteOptions options,
            List<StructField> inputFields) {
        final List<YdbFieldInfo> out = new ArrayList<>(inputFields.size());
        if (options.isMapByNames()) {
            for (StructField sf : inputFields) {
                final String name = sf.name();
                YdbFieldInfo yfi = options.getFieldsMap().get(name);
                if (yfi != null) {
                    out.add(yfi);
                }
            }
        } else {
            for (int pos = 0; pos < inputFields.size(); ++pos) {
                YdbFieldInfo yfi = options.getFieldsList().get(pos);
                out.add(yfi);
            }
        }
        return out;
    }

    private static String makeSql(YdbWriteOptions options, List<YdbFieldInfo> fields) {
        final StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $input AS List<Struct<");
        boolean comma = false;
        for (YdbFieldInfo f : fields) {
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
            case BULK:
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
        sb.append("`").append(escape(options.getTablePath())).append("`");
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

    private static StructType makeInputType(List<YdbFieldInfo> statementFields) {
        if (statementFields.isEmpty()) {
            throw new IllegalArgumentException("Empty input field list specified for writing");
        }
        final Map<String, Type> m = new HashMap<>();
        for (YdbFieldInfo yfi : statementFields) {
            m.put(yfi.getName(), YdbFieldType.toSdkType(yfi.getType(), yfi.isNullable()));
        }
        return StructType.of(m);
    }

}
