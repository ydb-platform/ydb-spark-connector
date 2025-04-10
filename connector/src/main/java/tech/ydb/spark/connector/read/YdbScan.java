package tech.ydb.spark.connector.read;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.common.KeysRange;

/**
 * Scan is the factory for the Batch.
 *
 * @author zinal
 */
public class YdbScan implements Scan, Batch, SupportsReportPartitioning {
    private static final Logger logger = LoggerFactory.getLogger(YdbScan.class);

    private final YdbTable table;
    private final YdbScanOptions options;

    public YdbScan(YdbTable table, YdbScanOptions options) {
        this.table = table;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return options.getReadSchema();
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public Partitioning outputPartitioning() {
        KeysRange[] partitions = table.getPartitions();

        // TODO: KeyGroupedPartitioning (requires HasPartitionKey for partitions)
        if (partitions.length == 0) {
            return new UnknownPartitioning(1);
        }
        return new UnknownPartitioning(partitions.length);
    }

    @Override
    public InputPartition[] planInputPartitions() {
        KeysRange predicateRange = options.getPredicateRange();

        KeysRange[] partitions = table.getPartitions();
        if (partitions.length == 0) {
            logger.warn("Missing partitioning information for table {}", table.getTablePath());
            // Single partition with possible limits taken from the predicates.
            return new InputPartition[] {new YdbTablePartition(0, KeysRange.UNRESTRICTED)};
        }
        logger.debug("Input table partitions: {}", Arrays.toString(partitions));
        final Random random = new Random();
        YdbTablePartition[] out = Stream.of(partitions)
                .map(kr -> kr.intersect(predicateRange))
                .filter(kr -> !kr.isEmpty())
                .map(kr -> new YdbTablePartition(random.nextInt(999999999), kr))
                .toArray(YdbTablePartition[]::new);
        if (logger.isDebugEnabled()) {
            logger.debug("Input partitions count {}, filtered partitions count {}", partitions.length, out.length);
            logger.debug("Filtered partition ranges: {}", Arrays.toString(out));
        }
        // Random ordering is better for multiple  concurrent scans with limited parallelism.
        Arrays.sort(out, (p1, p2) -> Integer.compare(p1.getOrderingKey(), p2.getOrderingKey()));
        return out;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new YdbReaderFactory(table, options);
    }
}
