package tech.ydb.spark.connector.write;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.IngestMethod;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.values.PrimitiveType;

/**
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public class YdbDataWriterFactory implements DataWriterFactory {
    private static final long serialVersionUID = -6000846276376311177L;

    private static final Logger logger = LoggerFactory.getLogger(YdbDataWriterFactory.class);

    private static final int MAX_ROWS_COUNT = 10000;
    private static final int MAX_BYTES_SIZE = 10 * 1024 * 1024;
    private static final int CONCURRENCY = 2;
    private static final int WRITE_RETRY_COUNT = 10;

    private final YdbTable table;
    private final YdbTypes types;
    private final StructType schema;

    private final IngestMethod method;
    private final boolean useApacheArrow;
    private final String autoPkName;
    private final int batchRowsCount;
    private final int batchBytesLimit;
    private final int batchConcurrency;
    private final int retryCount;

    public YdbDataWriterFactory(YdbTable table, StructType schema, CaseInsensitiveStringMap options) {
        this.table = table;
        this.types = new YdbTypes(options);
        this.method = OperationOption.INGEST_METHOD.readEnum(options, IngestMethod.BULK_UPSERT);
        this.useApacheArrow = OperationOption.USE_APACHE_ARROW.readBoolean(options, false);
        this.batchRowsCount = OperationOption.BATCH_ROWS.readInt(options, MAX_ROWS_COUNT);
        this.batchBytesLimit = OperationOption.BATCH_LIMIT.readInt(options, MAX_BYTES_SIZE);
        this.batchConcurrency = OperationOption.BATCH_CONCURRENCY.readInt(options, CONCURRENCY);
        this.autoPkName = OperationOption.TABLE_AUTOPK_NAME.read(options, OperationOption.DEFAULT_AUTO_PK);
        this.retryCount = OperationOption.WRITE_RETRY_COUNT.readInt(options, WRITE_RETRY_COUNT);
        this.schema = schema;

        if (useApacheArrow && method != IngestMethod.BULK_UPSERT) {
            logger.warn("Arrow ingestion was disabled because it is only supported with method BULK_UPSERT");
        }
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        logger.trace("New writer for table {}, partition {}, task {}", table.getTablePath(), partitionId, taskId);

        boolean idempotent = method != IngestMethod.INSERT;
        SessionRetryContext retryCtx = table.getCtx().getExecutor().createRetryCtx(retryCount, idempotent);
        YdbWriter writer = buildYdbWriter();
        return new YdbDataWriter(retryCtx, writer, batchConcurrency);
    }

    private YdbWriter buildYdbWriter() {
        List<ColumnEntry> columns = buildColumns();
        String tablePath = table.getTablePath();

        if (method == IngestMethod.BULK_UPSERT) {
            if (useApacheArrow) {
                return new YdbWriterArrow(tablePath, columns, batchRowsCount, batchBytesLimit);
            }
            return new YdbWriterBulkUpsert(tablePath, types, batchRowsCount, batchBytesLimit, columns);
        }

        return new YdbWriterDataQuery(method.name(), tablePath, types, batchRowsCount, batchBytesLimit, columns);
    }

    private List<ColumnEntry> buildColumns() {
        Map<String, FieldInfo> tableTypes = new HashMap<>();
        for (FieldInfo column : table.getAllColumns()) {
            tableTypes.put(column.getName(), column);
        }

        List<ColumnEntry> result = new ArrayList<>();
        Iterator<StructField> it = schema.iterator();
        int ordinal = 0;
        while (it.hasNext()) {
            StructField sf = it.next();
            String name = sf.name();
            FieldInfo fi = tableTypes.get(name);
            if (fi == null) {
                throw new IllegalArgumentException("Cannot write column " + name + " to table " + table);
            }
            result.add(new ColumnEntry(name, fi.getType(), sf.dataType(), ordinal++));
        }

        FieldInfo pk = tableTypes.get(autoPkName);
        if (pk != null) {
            if (pk.getSafeType() != PrimitiveType.Text) {
                throw new IllegalArgumentException("Wrong type of autopk column " + autoPkName + " -> " + pk.getType());
            }
            result.add(new ColumnEntry(autoPkName, pk.getType(), null, -1));
        }

        return result;
    }
}
