package tech.ydb.spark.connector.write;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
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
    private final StructType schema;
    private final CaseInsensitiveStringMap options;
    private final boolean truncate;

    private YdbWrite(YdbTable table, StructType schema, CaseInsensitiveStringMap options, boolean truncate) {
        this.table = table;
        this.schema = schema;
        this.options = options;
        this.truncate = truncate;
    }

    public YdbWrite(YdbTable table, LogicalWriteInfo info, boolean truncate) {
        this.table = table;
        this.schema = info.schema();
        Map<String, String> mergedOptions = new HashMap<>();
        mergedOptions.putAll(table.properties());
        mergedOptions.putAll(info.options());
        this.options = new CaseInsensitiveStringMap(mergedOptions);
        this.truncate = truncate;
    }

    @Override
    public Write build() {
        return this;
    }

    @Override
    public WriteBuilder truncate() {
        logger.info("Truncation requested for table {}", table.getTablePath());
        return new YdbWrite(table, schema, options, true);
    }

    @Override
    public BatchWrite toBatch() {
        logger.trace("YdbWrite converted to BatchWrite for table {}", table.getTablePath());
        return this;
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalInfo) {
        logger.trace("YdbWrite converted to DataWriterFactory for table {}", table.getTablePath());
        YdbDataWriterFactory factory = new YdbDataWriterFactory(table, schema, options);
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
}
