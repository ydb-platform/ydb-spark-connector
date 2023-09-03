package tech.ydb.spark.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Partition reader factory delivers the scan options to partition reader instances.
 *
 * @author zinal
 */
public class YdbPartitionReaderFactory implements PartitionReaderFactory {

    private final YdbScanOptions options;

    public YdbPartitionReaderFactory(YdbScanOptions options) {
        this.options = options;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new YdbPartitionReader(options, (YdbInputPartition) partition);
    }
}
