package tech.ydb.spark.connector.read;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.impl.YdbScanReadTable;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbReaderFactory implements PartitionReaderFactory {
    private static final Logger logger = LoggerFactory.getLogger(YdbReaderFactory.class);
    private static final long serialVersionUID = 6815846949510430713L;

    private final YdbTable table;
    private final YdbScanOptions options;

    public YdbReaderFactory(YdbTable table, YdbScanOptions options) {
        this.table = table;
        this.options = options;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new LazyReader(table, options, partition);
    }

    public static class LazyReader implements PartitionReader<InternalRow> {
        private final YdbTable table;
        private final YdbScanOptions options;
        private final YdbTablePartition partition;

        private YdbScanReadTable scan = null;

        public LazyReader(YdbTable table, YdbScanOptions options, InputPartition partition) {
            this.table = table;
            this.options = options;
            this.partition = (YdbTablePartition) partition;
        }

        @Override
        public boolean next() throws IOException {
            if (scan == null) {
                logger.debug("Preparing scan for table {} at partition {}", table.getTablePath(), partition);
                scan = new YdbScanReadTable(table, options, partition.getRange());
                logger.debug("Scan prepared, ready to fetch...");
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
                logger.debug("Closing the scan.");
                scan.close();
            }
            scan = null;
        }
    }

}
