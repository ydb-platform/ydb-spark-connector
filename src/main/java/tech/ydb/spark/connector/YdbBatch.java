package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 *
 * @author mzinal
 */
public class YdbBatch implements Batch {

    private final YdbScan scan;

    public YdbBatch(YdbScan scan) {
        this.scan = scan;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        if (scan.getOptions().getPartitions() == null
                || scan.getOptions().getPartitions().isEmpty()) {
            return new InputPartition[]{new YdbInputPartition()};
        }
        return scan.getOptions().getPartitions().stream()
                .map(kr -> new YdbInputPartition(kr)).toArray(InputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new YdbPartitionReaderFactory(scan.getOptions());
    }

}
