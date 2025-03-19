package tech.ydb.spark.connector.read;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.impl.YdbScanReadTable;
import tech.ydb.spark.connector.read.YdbScanOptions;

/**
 * Partition reader factory delivers the scan options to partition reader instances.
 *
 * @author zinal
 */
public class YdbPartitionReaderFactory implements PartitionReaderFactory {
    private static final long serialVersionUID = -3117498545638305126L;

    private static final Logger LOG = LoggerFactory.getLogger(YdbPartitionReaderFactory.class);


    private final YdbScanOptions options;

    public YdbPartitionReaderFactory(YdbScanOptions options) {
        this.options = options;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new YdbReader(options, (YdbTablePartition) partition);
    }

    static class YdbReader implements PartitionReader<InternalRow> {

        private final YdbScanOptions options;
        private final YdbTablePartition partition;
        private YdbScanReadTable scan;

        YdbReader(YdbScanOptions options, YdbTablePartition partition) {
            this.options = options;
            this.partition = partition;
        }

        @Override
        public boolean next() throws IOException {
            if (scan == null) {
                LOG.debug("Preparing scan for table {} at partition {}",
                        options.getTablePath(), partition);
                scan = new YdbScanReadTable(options, partition.getRange());
                LOG.debug("Scan prepared, ready to fetch...");
            }
            return scan.next();
        }

        @Override
        public InternalRow get() {
            return scan.get();
        }

        @Override
        public void close() throws IOException {
            if (scan != null) {
                LOG.debug("Closing the scan.");
                scan.close();
            }
            scan = null;
        }
    }

}
