package tech.ydb.spark.connector;

import java.io.Serializable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import tech.ydb.spark.connector.impl.YdbTruncateTable;
import tech.ydb.spark.connector.impl.YdbWriterImpl;

/**
 * YDB table writer: orchestration and partition writer factory.
 *
 * @author zinal
 */
public class YdbWrite implements Serializable, Write, BatchWrite, DataWriterFactory {

    private static final long serialVersionUID = 1L;

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbWrite.class);

    private final YdbWriteOptions options;

    YdbWrite(YdbWriteOptions options) {
        this.options = options;
    }

    YdbWrite(YdbTable table, LogicalWriteInfo lwi, boolean mapByNames, boolean truncate) {
        this(new YdbWriteOptions(table, mapByNames, lwi.schema(), lwi.queryId(), lwi.options(), truncate));
    }

    @Override
    public BatchWrite toBatch() {
        LOG.debug("YdbWrite converted to BatchWrite for table {}", options.getTablePath());
        return this;
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        LOG.debug("YdbWrite converted to DataWriterFactory for table {}", options.getTablePath());
        // TODO: create the COW copy of the destination table
        if (options.isTruncate()) {
            YdbTruncateTable action = new YdbTruncateTable(options.getTablePath());
            options.grabConnector().getRetryCtx()
                    .supplyStatus(session -> action.run(session))
                    .join()
                    .expectSuccess();
        }
        return this;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        // TODO: replace the original table with its copy
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // TODO: remove the COW copy
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        LOG.debug("YdbWriterImpl created for table {}, partition {}, task {}",
                options.getTablePath(), partitionId, taskId);
        return new YdbWriterImpl(options, partitionId, taskId);
    }

}
