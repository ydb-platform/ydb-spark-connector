package tech.ydb.spark.connector;

import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author zinal
 */
public class YdbScanBuilder implements ScanBuilder,
        SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbScanBuilder.class);

    private final YdbTable table;
    private final YdbScanOptions options;

    public YdbScanBuilder(YdbTable table) {
        this.table = table;
        this.options = new YdbScanOptions(table);
        LOG.debug("Preparing scan for table {}", table);
    }

    @Override
    public Scan build() {
        return new YdbScan(table, options);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        LOG.debug("Pushing filters {}", filters);
        return options.pushFilters(filters);
    }

    @Override
    public Filter[] pushedFilters() {
        return options.pushedFilters();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        LOG.debug("Required columns {}", requiredSchema);
        options.pruneColumns(requiredSchema);
    }

    public static class YdbScan implements Scan  {
        private final YdbTable table;
        private final YdbScanOptions options;

        public YdbScan(YdbTable table, YdbScanOptions options) {
            this.table = table;
            this.options = options;
        }

        public YdbTable getTable() {
            return table;
        }

        public YdbScanOptions getOptions() {
            return options;
        }

        @Override
        public StructType readSchema() {
            return options.readSchema();
        }

        @Override
        public Batch toBatch() {
            return new YdbBatch(this);
        }
    }

    public static class YdbBatch implements Batch {

        private final YdbScan scan;

        public YdbBatch(YdbScan scan) {
            this.scan = scan;
        }

        @Override
        public InputPartition[] planInputPartitions() {
            // No partitioning right now, single scan per table.
            return new InputPartition[]{ new YdbInputPartition() };
        }

        @Override
        public PartitionReaderFactory createReaderFactory() {
            return new YdbPartitionReaderFactory(scan);
        }

    }

    public static class YdbInputPartition implements InputPartition {
        // Scans are not partitioned (yet?)
    }

    public static class YdbPartitionReaderFactory implements PartitionReaderFactory {

        private final YdbScanOptions options;

        public YdbPartitionReaderFactory(YdbScan scan) {
            this.options = scan.getOptions();
        }

        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            // Partition is ignored, scans are not partitioned (yet?)
            return new YdbPartitionReader(options);
        }

    }

    public static class YdbPartitionReader implements PartitionReader<InternalRow> {

        private final YdbScanOptions options;
        private YdbReadTable query;

        public YdbPartitionReader(YdbScanOptions options) {
            this.options = options;
        }

        @Override
        public boolean next() throws IOException {
            if (query==null) {
                LOG.debug("Preparing scan for table {}", options.getTableName());
                query = new YdbReadTable(options);
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
            if (query!=null) {
                LOG.debug("Closing the scan.");
                query.close();
            }
            query = null;
        }

    }
}
