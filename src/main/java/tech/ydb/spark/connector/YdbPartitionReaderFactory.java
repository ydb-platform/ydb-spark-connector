package tech.ydb.spark.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 *
 * @author zinal
 */
public class YdbPartitionReaderFactory implements PartitionReaderFactory {

    private final YdbScan scan;

    public YdbPartitionReaderFactory(YdbScan scan) {
        this.scan = scan;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new YdbPartitionReader(scan, (YdbInputPartition) partition);
    }

}
