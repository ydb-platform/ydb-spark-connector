package tech.ydb.spark.connector;

import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

/**
 * Partition reader implements partition scan invocation.
 *
 * @author zinal
 */
public class YdbPartitionReader implements PartitionReader<InternalRow> {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbPartitionReader.class);

    private final YdbScanOptions options;
    private final YdbTablePartition partition;
    private YdbViaReadTable query;

    public YdbPartitionReader(YdbScanOptions options, YdbTablePartition partition) {
        this.options = options;
        this.partition = partition;
    }

    @Override
    public boolean next() throws IOException {
        if (query == null) {
            LOG.debug("Preparing scan for table {} at partition {}",
                    options.getTablePath(), partition);
            query = new YdbViaReadTable(options, partition.getRange());
            query.prepare();
            LOG.debug("Scan prepared, ready to fetch...");
        }
        return query.next();
    }

    @Override
    public InternalRow get() {
        return query.get();
    }

    @Override
    public void close() throws IOException {
        if (query != null) {
            LOG.debug("Closing the scan.");
            query.close();
        }
        query = null;
    }

}
