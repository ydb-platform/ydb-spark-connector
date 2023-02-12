package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author zinal
 */
public class YdbScanBuilder implements ScanBuilder,
        SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private final YdbTable table;
    private final YdbScanOptions options;

    public YdbScanBuilder(YdbTable table) {
        this.table = table;
        this.options = new YdbScanOptions(table.getDescription());
    }

    @Override
    public Scan build() {
        return new YdbScan(table, new YdbScanOptions(options));
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        return options.pushFilters(filters);
    }

    @Override
    public Filter[] pushedFilters() {
        return options.pushedFilters();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        options.pruneColumns(requiredSchema);
    }

}
