package tech.ydb.spark.connector.write;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import tech.ydb.core.Status;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.IngestMethod;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbWriterFactory implements DataWriterFactory {
    private static final long serialVersionUID = -6000846276376311177L;
    private static final Logger logger = LoggerFactory.getLogger(YdbWriterFactory.class);

    private final YdbTable table;
    private final YdbTypes types;
    private final org.apache.spark.sql.types.StructType schema;

    private final IngestMethod method;
    private final String autoPkName;
    private final int batchRowsCount;
    private final int batchBytesLimit;
    private final int batchConcurrency;

    public YdbWriterFactory(YdbTable table, LogicalWriteInfo logical, PhysicalWriteInfo physical) {
        this.table = table;
        this.types = new YdbTypes(logical.options());
        this.method = OperationOption.INGEST_METHOD.readEnum(logical.options(), IngestMethod.BULK_UPSERT);
        this.batchRowsCount = OperationOption.BATCH_ROWS.readInt(logical.options(), YdbDataWriter.MAX_ROWS_COUNT);
        this.batchBytesLimit = OperationOption.BATCH_LIMIT.readInt(logical.options(), YdbDataWriter.MAX_BYTES_SIZE);
        this.batchConcurrency = OperationOption.BATCH_CONCURRENCY.readInt(logical.options(), YdbDataWriter.CONCURRENCY);
        this.autoPkName = OperationOption.TABLE_AUTOPK_NAME.read(logical.options(), OperationOption.DEFAULT_AUTO_PK);
        this.schema = logical.schema();
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        logger.debug("New writer for table {}, partition {}, task {}", table.getTablePath(), partitionId, taskId);

        Map<String, FieldInfo> tableTypes = new HashMap<>();
        for (FieldInfo column : table.getAllColumns()) {
            tableTypes.put(column.getName(), column);
        }

        Map<String, Type> inputTypes = new HashMap<>();
        Map<String, ValueReader> columnReadeds = new HashMap<>();

        Iterator<StructField> it = schema.toIterator();
        int idx = 0;
        while (it.hasNext()) {
            StructField sf = it.next();
            String name = sf.name();
            if (!tableTypes.containsKey(name)) {
                throw new IllegalArgumentException("Cannot write column " + name + " to table " + table);
            }
            FieldInfo fi = tableTypes.get(name);
            inputTypes.put(name, fi.getType());
            columnReadeds.put(name, new ColumnReader(idx++, sf.dataType(), fi.getType()));
        }

        if (tableTypes.containsKey(autoPkName)) {
            FieldInfo fi = tableTypes.get(autoPkName);
            if (fi.getSafeType() != PrimitiveType.Text) {
                throw new IllegalArgumentException("Wrong type of autopk column "
                        + autoPkName + " -> " + fi.getType());
            }

            inputTypes.put(autoPkName, fi.getType());
            columnReadeds.put(autoPkName, new RandomReader());
        }

        StructType structType = StructType.of(inputTypes);
        ValueReader[] readers = new ValueReader[structType.getMembersCount()];
        for (idx = 0; idx < structType.getMembersCount(); idx += 1) {
            readers[idx] = columnReadeds.get(structType.getMemberName(idx));
        }

        if (method == IngestMethod.BULK_UPSERT) {
            return new YdbDataWriter(types, structType, readers, batchRowsCount, batchBytesLimit, batchConcurrency) {
                @Override
                CompletableFuture<Status> executeWrite(ListValue batch) {
                    return table.getCtx().getExecutor().executeBulkUpsert(table.getTablePath(), batch);
                }
            };
        }

        String writeQuery = makeBatchSql(method.name(), table.getTablePath(), structType);
        return new YdbDataWriter(types, structType, readers, batchRowsCount, batchBytesLimit, batchConcurrency) {
            @Override
            CompletableFuture<Status> executeWrite(ListValue batch) {
                return table.getCtx().getExecutor().executeDataQuery(writeQuery, Params.of("$input", batch));
            }
        };
    }

    private static String makeBatchSql(String command, String tablePath, StructType st) {
        final StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $input AS List<Struct<");
        for (int idx = 0; idx < st.getMembersCount(); idx += 1) {
            sb.append('`').append(st.getMemberName(idx)).append('`');
            sb.append(": ").append(st.getMemberType(idx));
            if (idx + 1 < st.getMembersCount()) {
                sb.append(", ");
            }
        }
        sb.append(">>;\n");
        sb.append(command).append(" INTO `").append(tablePath).append("`  SELECT * FROM AS_TABLE($input);");
        return sb.toString();
    }
}
