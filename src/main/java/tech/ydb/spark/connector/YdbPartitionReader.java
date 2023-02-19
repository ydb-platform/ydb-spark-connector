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
    private YdbReadTable query;

    public YdbPartitionReader(YdbScanOptions options, YdbInputPartition partition) {
        this.options = options;
        this.partition = partition;
    }

    @Override
    public boolean next() throws IOException {
        if (query==null) {
            query = new YdbReadTable(options, partition);
            query.prepare();
        }
        return query.next();
    }

    @Override
    public InternalRow get() {
        return query.get();
    }

    @Override
    public void close() throws IOException {
        if (query!=null)
            query.close();
        query = null;
    }

}
