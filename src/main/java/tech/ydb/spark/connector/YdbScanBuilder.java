package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author zinal
 */
public class YdbScanBuilder implements ScanBuilder,
        SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns,
        SupportsPushDownLimit {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbScanBuilder.class);

    private final YdbTable table;
    private final YdbScanOptions options;

    public YdbScanBuilder(YdbTable table) {
        this.table = table;
        this.options = new YdbScanOptions(table);
        LOG.debug("Preparing scan for table {}", table);
    }

    @Override
    public Scan build() {
        return new YdbScan(table, options);
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        options.setupPredicates(predicates);
        return predicates; // all predicates should be re-checked
    }

    @Override
    public Predicate[] pushedPredicates() {
        return new Predicate[]{}; // all predicates should be re-checked
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        options.pruneColumns(requiredSchema);
    }

    @Override
    public boolean pushLimit(int count) {
        options.setRowLimit(count);
        return false;
    }
}
