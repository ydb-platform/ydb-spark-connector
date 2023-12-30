package tech.ydb.spark.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import scala.collection.JavaConverters;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;

import tech.ydb.core.Status;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB table writer: basic row writer.
 *
 * @author zinal
 */
public class YdbWriterBasic implements DataWriter<InternalRow> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbWriterBasic.class);

    private final YdbTypes types;
    private final List<StructField> sparkFields;
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

    public YdbWriterBasic(YdbWriteOptions options) {
        this.types = options.getTypes();
        this.sparkFields = new ArrayList<>(JavaConverters.asJavaCollection(
                options.getInputType().toList()));
        this.statementFields = makeStatementFields(options, this.sparkFields);
        this.sqlStatement = makeSql(options, this.statementFields);
        this.tablePath = options.getTablePath();
        this.inputType = makeInputType(this.statementFields);
        this.listType = tech.ydb.table.values.ListType.of(this.inputType);
        this.ingestMethod = options.getIngestMethod();
        this.maxBulkRows = options.getMaxBulkRows();
        this.connector = YdbRegistry.getOrCreate(
                options.getCatalogName(), options.getConnectOptions());
        this.currentInput = new ArrayList<>();
        this.currentStatus = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("YQL statement: {}", this.sqlStatement);
            LOG.debug("Input structure: {}", this.inputType);
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
        if (numFields != sparkFields.size()) {
            throw new IOException("Incorrect input record field count, expected "
                    + Integer.toString(sparkFields.size()) + ", actual "
                    + Integer.toString(record.numFields()));
        }
        Map<String, Value<?>> currentRow = new HashMap<>();
        for (int i = 0; i < numFields; ++i) {
            YdbFieldInfo yfi = statementFields.get(i);
            final Object value = record.get(i, sparkFields.get(i).dataType());
            Value<?> conv = types.convertToYdb(value, yfi.getType());
            if (yfi.isNullable()) {
                if (conv.getType().getKind() != Type.Kind.OPTIONAL)
                    conv = conv.makeOptional();
            }
            currentRow.put(yfi.getName(), conv);
        }
        // LOG.debug("Converted input row: {}", currentRow);
        return currentRow;
    }

    private void startNewStatement(List<Value<?>> input) {
        if (currentStatus!=null) {
            currentStatus.join().expectSuccess();
            currentStatus = null;
        }
        // The list is being copied here.
        // DO NOT move this call into the async methods below.
        ListValue value = listType.newValue(input);
        if (YdbIngestMethod.BULK.equals(ingestMethod)) {
            currentStatus = connector.getRetryCtx().supplyStatus(
                    session -> session.executeBulkUpsert(
                            tablePath, value, new BulkUpsertSettings()));
        } else {
            currentStatus = connector.getRetryCtx().supplyStatus(
                    session -> CompletableFuture.completedFuture(
                            session.executeDataQuery(sqlStatement,
                                    TxControl.serializableRw().setCommitTx(true),
                                    Params.of("$input", value))
                                    .join().getStatus()));
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // Process the currently prepared set of rows
        if (! currentInput.isEmpty()) {
            startNewStatement(currentInput);
            currentInput.clear();
        }
        // Wait for the statement to be executed, and check the results
        if (currentStatus!=null) {
            currentStatus.join().expectSuccess();
            currentStatus = null;
        }
        // All rows have been written successfully
        return new YdbWriteCommit();
    }

    @Override
    public void abort() throws IOException {
        currentInput.clear();
        if (currentStatus!=null) {
            currentStatus.cancel(true);
            currentStatus.join();
            currentStatus = null;
        }
    }

    @Override
    public void close() throws IOException {
        currentInput.clear();
        if (currentStatus!=null) {
            currentStatus.cancel(true);
            currentStatus.join();
            currentStatus = null;
        }
    }

    private static List<YdbFieldInfo> makeStatementFields(YdbWriteOptions options,
            List<StructField> sparkFields) {
        final List<YdbFieldInfo> out = new ArrayList<>(sparkFields.size());
        for (StructField sf : sparkFields) {
            final String name = sf.name();
            YdbFieldInfo fieldInfo = options.getFields().get(name);
            if (fieldInfo!=null) {
                out.add(fieldInfo);
            }
        }
        return out;
    }

    private static String makeSql(YdbWriteOptions options, List<YdbFieldInfo> fields) {
        final StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $input AS List<Struct<");
        boolean comma = false;
        for (YdbFieldInfo f : fields) {
            if (comma) sb.append(", "); else comma = true;
            sb.append("`").append(f.getName()).append("`");
            sb.append(": ");
            sb.append(f.getType().sqlName);
            if (f.isNullable())
                sb.append("?");
        }
        sb.append(">>;\n");
        switch (options.getIngestMethod()) {
            case BULK: /* unused sql statement, so use UPSERT to generate it */
            case UPSERT:
                sb.append("UPSERT INTO ");
                break;
            case REPLACE:
                sb.append("REPLACE INTO ");
                break;
        }
        sb.append("`").append(options.getTablePath()).append("`");
        sb.append(" SELECT * FROM AS_TABLE($input);");
        return sb.toString();
    }

    private static StructType makeInputType(List<YdbFieldInfo> statementFields) {
        final Map<String,Type> m = new HashMap<>();
        for (YdbFieldInfo yfi : statementFields) {
            m.put(yfi.getName(), YdbFieldType.toSdkType(yfi.getType(), yfi.isNullable()));
        }
        if (m.isEmpty()) {
            throw new IllegalArgumentException("Empty input field list specified for writing");
        }
        return StructType.of(m);
    }

}
