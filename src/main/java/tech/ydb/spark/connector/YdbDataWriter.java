package tech.ydb.spark.connector;

import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * YDB table writer: basic row writer.
 *
 * @author zinal
 */
public class YdbDataWriter implements DataWriter<InternalRow> {

    private final YdbUpsertOptions options;

    public YdbDataWriter(YdbUpsertOptions options) {
        this.options = options;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        // TODO
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void abort() throws IOException {
        // TODO
    }

    @Override
    public void close() throws IOException {
        // TODO
    }

}
