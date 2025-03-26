package tech.ydb.spark.connector.read;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.common.KeysRange;


/**
 * Per-partition scan planning, including the partition elimination logic.
 *
 * @author zinal
 */
public class YdbScanBatch implements Batch {

    private static final Logger LOG = LoggerFactory.getLogger(YdbScanBatch.class);

    private final YdbScanOptions options;

    public YdbScanBatch(YdbScanOptions options) {
        this.options = options;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<KeysRange> partitions = options.getPartitions();
        if (partitions == null || partitions.isEmpty()) {
            // Single partition with possible limits taken from the predicates.
            partitions = new ArrayList<>(1);
            partitions.add(KeysRange.UNRESTRICTED);
            LOG.warn("Missing partitioning information for table {}", options.getTablePath());
        }
        LOG.debug("Input table partitions: {}", partitions);
        // Predicates restriction
        KeysRange predicates = new KeysRange(
                new KeysRange.Limit(options.getRangeBegin(), true),
                new KeysRange.Limit(options.getRangeEnd(), true)
        );
        final Random random = new Random();
        YdbTablePartition[] out = partitions.stream()
                .map(kr -> kr.intersect(predicates))
                .filter(kr -> !kr.isEmpty())
                .map(kr -> new YdbTablePartition(random.nextInt(999999999), kr))
                .toArray(YdbTablePartition[]::new);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Input partitions count {}, filtered partitions count {}",
                    partitions.size(), out.length);
            LOG.debug("Filtered partition ranges: {}", Arrays.toString(out));
        }
        // Random ordering is better for multiple  concurrent scans with limited parallelism.
        Arrays.sort(out, (p1, p2) -> Integer.compare(p1.getOrderingKey(), p2.getOrderingKey()));
        return out;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new YdbPartitionReaderFactory(options);
    }

}
