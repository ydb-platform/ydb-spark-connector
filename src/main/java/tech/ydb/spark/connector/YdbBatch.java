package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 *
 * @author zinal
 */
public class YdbBatch implements Batch {

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
