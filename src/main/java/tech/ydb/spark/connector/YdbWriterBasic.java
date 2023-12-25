package tech.ydb.spark.connector;

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
import tech.ydb.core.Result;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;

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

    public final static int MAX_ROWS = 500;

    private final YdbTypes types;
    private final List<StructField> sparkFields;
    private final List<YdbFieldInfo> statementFields;
    private final String sqlUpsert;
    private final tech.ydb.table.values.StructType inputType;
    private final tech.ydb.table.values.ListType listType;

    private final YdbConnector connector;
    private final List<Value<?>> currentInput;
    private CompletableFuture<Result<DataQueryResult>> currentResult;

    public YdbWriterBasic(YdbWriteOptions options) {
        this.types = options.getTypes();
        this.sparkFields = new ArrayList<>(JavaConverters.asJavaCollection(
                options.getInputType().toList()));
        this.statementFields = makeStatementFields(options, this.sparkFields);
        this.sqlUpsert = makeSqlUpsert(options, this.statementFields);
        this.inputType = makeInputType(this.statementFields);
        this.listType = tech.ydb.table.values.ListType.of(this.inputType);
        this.connector = YdbRegistry.getOrCreate(
                options.getCatalogName(), options.getConnectOptions());
        this.currentInput = new ArrayList<>();
        this.currentResult = null;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        final int numFields = record.numFields();
        if (numFields != sparkFields.size()) {
            throw new IOException("Incorrect input record field count, expected "
                    + Integer.toString(sparkFields.size()) + ", actual "
                    + Integer.toString(record.numFields()));
        }
        final Map<String, Value<?>> currentRow = new HashMap<>();
        for (int i = 0; i < numFields; ++i) {
            YdbFieldInfo yfi = statementFields.get(i);
            final Object value = record.get(i, sparkFields.get(i).dataType());
            Value<?> conv = types.convertToYdb(value, yfi.getType());
            if (yfi.isNullable()) {
                if (conv.getType().getKind() != Type.Kind.OPTIONAL)
                    conv = conv.makeOptional();
            }
            currentRow.put(sqlUpsert, conv);
        }
        currentInput.add(StructValue.of(currentRow));
        if (currentInput.size() >= MAX_ROWS) {
            startNewStatement(currentInput);
            currentInput.clear();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if (currentResult!=null) {
            currentResult.join().getStatus().expectSuccess();
            currentResult = null;
        }
        return new YdbWriteCommit();
    }

    @Override
    public void abort() throws IOException {
        // TODO
    }

    @Override
    public void close() throws IOException {
        // TODO
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

    private static String makeSqlUpsert(YdbWriteOptions options, List<YdbFieldInfo> fields) {
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
        sb.append("UPSERT INTO `").append(options.getTablePath()).append("`");
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

    private void startNewStatement(List<Value<?>> input) {
        if (currentResult!=null) {
            currentResult.join().getStatus().expectSuccess();
        }
        Params params = Params.of("$input", listType.newValue(input));
        currentResult = connector.getRetryCtx().supplyResult(session -> session.executeDataQuery(
                            sqlUpsert, TxControl.serializableRw().setCommitTx(true), params));
    }

}
