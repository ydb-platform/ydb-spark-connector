package tech.ydb.spark.connector;

import java.io.Serializable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import tech.ydb.spark.connector.impl.YdbWriterImpl;

/**
 * YDB table writer: orchestration and partition writer factory.
 *
 * @author zinal
 */
public class YdbWrite implements Serializable,
        WriteBuilder, Write, BatchWrite, DataWriterFactory {

    private static final long serialVersionUID = 1L;

    private final YdbWriteOptions options;

    YdbWrite(YdbTable table, LogicalWriteInfo lwi, boolean mapByNames) {
        this.options = new YdbWriteOptions(table, mapByNames,
                lwi.schema(), lwi.queryId(), lwi.options());
    }

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
        // TODO: create the COW copy of the destination table
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
        return new YdbWriterImpl(options);
    }

}
