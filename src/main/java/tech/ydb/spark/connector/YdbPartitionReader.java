package tech.ydb.spark.connector;

import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

/**
 *
 * @author zinal
 */
public class YdbPartitionReader implements PartitionReader<InternalRow> {

    private final YdbScanOptions options;
    private final YdbInputPartition partition;

    public YdbPartitionReader(YdbScanOptions options, YdbInputPartition partition) {
        this.options = options;
        this.partition = partition;
    }

    @Override
    public boolean next() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public InternalRow get() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
