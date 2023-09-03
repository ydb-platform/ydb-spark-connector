package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

/**
 *  Scan is the factory for the Batch.
 *
 * @author zinal
 */
public class YdbScan implements Scan {

    private final YdbTable table;
    private final YdbScanOptions options;

    public YdbScan(YdbTable table, YdbScanOptions options) {
        this.table = table;
        this.options = options;
    }

    public YdbTable getTable() {
        return table;
    }

    public YdbScanOptions getOptions() {
        return options;
    }

    @Override
    public StructType readSchema() {
        return options.readSchema();
    }

    @Override
    public Batch toBatch() {
        return new YdbBatch(options);
    }
}
