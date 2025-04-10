package tech.ydb.spark.connector.write;


import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.YdbTable;

/**
 * YDB table writer: orchestration and partition writer factory.
 *
 * @author zinal
 */
public class YdbWrite implements WriteBuilder, SupportsTruncate, Write, BatchWrite {
    private static final Logger logger = LoggerFactory.getLogger(YdbWrite.class);

    private final YdbTable table;
    private final LogicalWriteInfo logicalInfo;
    private final boolean truncate;

    public YdbWrite(YdbTable table, LogicalWriteInfo info, boolean truncate) {
        this.table = table;
        this.logicalInfo = info;
        this.truncate = truncate;
    }

    @Override
    public Write build() {
        return this;
    }

    @Override
    public WriteBuilder truncate() {
        logger.debug("Truncation requested for table {}", table.getTablePath());
        return new YdbWrite(table, logicalInfo, true);
    }

    @Override
    public BatchWrite toBatch() {
        logger.debug("YdbWrite converted to BatchWrite for table {}", table.getTablePath());
        return this;
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalInfo) {
        logger.debug("YdbWrite converted to DataWriterFactory for table {}", table.getTablePath());
        YdbWriterFactory factory = new YdbWriterFactory(table, logicalInfo, physicalInfo);
        // TODO: create the COW copy of the destination table
        if (truncate) {
            table.truncateTable();
        }
        return factory;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        // TODO: replace the original table with its copy
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // TODO: remove the COW copy
    }
/*
    private boolean validateSchemas(StructType actualSchema, StructType inputSchema) {
        final List<StructField> inputFields = new ArrayList<>(JavaConverters.asJavaCollection(inputSchema.toList()));
        if (mapByNames) {
            for (StructField sfi : inputFields) {
                Option<Object> index = actualSchema.getFieldIndex(sfi.name());
                if (!index.isDefined() || index.isEmpty()) {
                    throw new IllegalArgumentException("Ingestion input cannot be mapped by names: "
                            + "unknown field [" + sfi.name() + "] specified on input.");
                }
                StructField sfa = actualSchema.fields()[(int) index.get()];

                if (!isAssignableFrom(sfa.dataType(), sfi.dataType())) {
                    throw new IllegalArgumentException("Ingestion input cannot be converted: "
                            + "field [" + sfi.name() + "] cannot be converted to type "
                            + sfa.dataType() + " from type " + sfi.dataType());
                }
            }
        } else {
            if (actualSchema.size() != inputSchema.size()) {
                throw new IllegalArgumentException("Ingestion input cannot be mapped by position: "
                        + "expected " + String.valueOf(actualSchema.size()) + " fields, "
                        + "got " + inputSchema.size() + " fields.");
            }
        }
        return mapByNames;
    }

    private static boolean areNamesAutoGenerated(List<StructField> inputFields) {
        for (StructField sf : inputFields) {
            if (!p.matcher(sf.name()).matches()) {
                return false;
            }
        }
        return true;
    }

    private boolean isAssignableFrom(DataType dst, DataType src) {
        // TODO: validate data type compatibility
        return true;
    }
*/
}
