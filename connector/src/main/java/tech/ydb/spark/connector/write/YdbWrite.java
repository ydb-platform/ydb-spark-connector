package tech.ydb.spark.connector.write;

import java.io.Serializable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.impl.YdbTruncateTable;

/**
 * YDB table writer: orchestration and partition writer factory.
 *
 * @author zinal
 */
public class YdbWrite implements Serializable, Write, BatchWrite, DataWriterFactory {
    private static final long serialVersionUID = 3457488224723758266L;

    private static final Logger LOG = LoggerFactory.getLogger(YdbWrite.class);

    private final YdbWriteOptions options;

    public YdbWrite(YdbTable table, LogicalWriteInfo lwi, boolean mapByNames, boolean truncate) {
        this.options = new YdbWriteOptions(table, mapByNames, lwi.schema(), lwi.queryId(), lwi.options(), truncate);
    }

    @Override
    public BatchWrite toBatch() {
        LOG.debug("YdbWrite converted to BatchWrite for table {}", options.getTable().getTablePath());
        return this;
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        LOG.debug("YdbWrite converted to DataWriterFactory for table {}", options.getTable().getTablePath());
        // TODO: create the COW copy of the destination table
        if (options.isTruncate()) {
            YdbTruncateTable action = new YdbTruncateTable(options.getTable().getTablePath());
            options.getTable().getConnector().getRetryCtx()
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
                options.getTable().getTablePath(), partitionId, taskId);
        return new YdbWriterImpl(options, partitionId, taskId);
    }

}
