package tech.ydb.spark.connector.read;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import tech.ydb.spark.connector.YdbTable;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbReadTableReaderFactory implements PartitionReaderFactory {
    private static final long serialVersionUID = 6815846949510430713L;

    private final YdbTable table;
    private final YdbReadTableOptions options;

    public YdbReadTableReaderFactory(YdbTable table, YdbReadTableOptions options) {
        this.table = table;
        this.options = options;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        YdbTablePartition p = (YdbTablePartition) partition;
        return new YdbReadTableReader(table, options, p.getRange());
    }
}
