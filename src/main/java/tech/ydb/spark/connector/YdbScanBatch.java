package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Per-partition scan planning, including the partition elimination logic.
 *
 * @author zinal
 */
public class YdbScanBatch implements Batch {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbScanBatch.class);

    private final YdbScanOptions options;

    public YdbScanBatch(YdbScanOptions options) {
        this.options = options;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<YdbKeyRange> partitions = options.getPartitions();
        if (partitions == null || partitions.isEmpty()) {
            // Single partition with possible limits taken from the predicates.
            partitions = new ArrayList<>(1);
            partitions.add(YdbKeyRange.UNRESTRICTED);
            LOG.warn("Missing partitioning information for table {}", options.getTablePath());
        }
        LOG.debug("Input table partitions: {}", partitions);
        // Predicates restriction
        YdbKeyRange predicates = new YdbKeyRange(
                new YdbKeyRange.Limit(options.getRangeBegin(), true),
                new YdbKeyRange.Limit(options.getRangeEnd(), true)
        );
        InputPartition[] out = partitions.stream()
                .map(kr -> kr.intersect(predicates))
                .filter(kr -> ! kr.isEmpty())
                .map(kr -> new YdbTablePartition(kr))
                .toArray(InputPartition[]::new);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Input partitions count {}, filtered partitions count {}",
                    partitions.size(), out.length);
            LOG.debug("Filtered partition ranges: {}", Arrays.toString(out));
        }
        return out;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new YdbPartitionReaderFactory(options);
    }

}
