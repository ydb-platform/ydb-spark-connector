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

    private final YdbScanOptions options;

    public YdbPartitionReaderFactory(YdbScan scan) {
        this.options = scan.getOptions();
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new YdbPartitionReader(options, (YdbInputPartition) partition);
    }

}
