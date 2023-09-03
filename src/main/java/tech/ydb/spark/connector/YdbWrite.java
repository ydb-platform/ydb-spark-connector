package tech.ydb.spark.connector;

import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.*;

/**
 *
 * @author zinal
 */
public class YdbWrite implements Serializable, WriteBuilder, Write, BatchWrite, DataWriterFactory {

    @Override
    public Write build() {
        return this;
    }

    @Override
    public BatchWrite toBatch() {
        return this;
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return this;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new YdbDataWriter();
    }

}
