package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 *
 * @author mzinal
 */
public class YdbBatch implements Batch {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbBatch.class);

    private final YdbScanOptions options;

    public YdbBatch(YdbScanOptions options) {
        this.options = options;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<YdbKeyRange> partitions = options.getPartitions();
        if (partitions == null || partitions.isEmpty()) {
            // Single partition with possible limits taken from the predicates.
            partitions = new ArrayList<>(1);
            partitions.add(YdbKeyRange.UNRESTRICTED);
        }
        // Predicates restriction
        YdbKeyRange predicates = new YdbKeyRange(
                new YdbKeyRange.Limit(options.getRangeBegin(), true),
                new YdbKeyRange.Limit(options.getRangeEnd(), true)
        );
        InputPartition[] out = partitions.stream()
                .map(kr -> kr.intersect(predicates))
                .filter(kr -> ! kr.isEmpty())
                .map(kr -> new YdbInputPartition(kr))
                .toArray(InputPartition[]::new);
        LOG.debug("Filtered input partitions: {}", out);
        return out;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new YdbPartitionReaderFactory(options);
    }

}
