package tech.ydb.spark.connector;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import tech.ydb.spark.connector.impl.YdbReadAbstract;
import tech.ydb.spark.connector.impl.YdbReadViaReadTable;

/**
 * Partition reader factory delivers the scan options to partition reader instances.
 *
 * @author zinal
 */
public class YdbPartitionReaderFactory implements PartitionReaderFactory {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbPartitionReaderFactory.class);

    private static final long serialVersionUID = 1L;

    private final YdbScanOptions options;

    public YdbPartitionReaderFactory(YdbScanOptions options) {
        this.options = options;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new YdbReader(options, (YdbTablePartition) partition);
    }

    static YdbReadAbstract newScan(YdbScanOptions options, YdbTablePartition partition) {
        // TODO: switch to the chosen implementation based on the options.
        return new YdbReadViaReadTable(options, partition.getRange());
    }

    static class YdbReader implements PartitionReader<InternalRow> {

        private final YdbScanOptions options;
        private final YdbTablePartition partition;
        private YdbReadAbstract scan;

        YdbReader(YdbScanOptions options, YdbTablePartition partition) {
            this.options = options;
            this.partition = partition;
        }

        @Override
        public boolean next() throws IOException {
            if (scan == null) {
                LOG.debug("Preparing scan for table {} at partition {}",
                        options.getTablePath(), partition);
                scan = newScan(options, partition);
                scan.prepare();
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
