package tech.ydb.spark.connector.read;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.types.StructType;

import tech.ydb.spark.connector.YdbTable;

/**
 * Scan is the factory for the Batch.
 *
 * @author zinal
 */
public class YdbScanTable implements Scan, Batch, SupportsReportPartitioning, PartitionReaderFactory {

    private static final long serialVersionUID = -6215949064280861219L;
    private final YdbTable table;
    private final YdbScanTableOptions options;

    public YdbScanTable(YdbTable table, YdbScanTableOptions options) {
        this.table = table;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return options.getReadSchema();
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        TabletPartition p = (TabletPartition) partition;
        return new YdbScanTableReader(table, options, p.getTablet());
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return this;
    }

    @Override
    public Partitioning outputPartitioning() {
        return new UnknownPartitioning(1);
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[]{new TabletPartition(-1)};
    }
}
