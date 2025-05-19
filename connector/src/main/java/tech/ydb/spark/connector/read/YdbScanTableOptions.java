package tech.ydb.spark.connector.read;

import java.io.Serializable;

import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;

/**
 * All settings for the scan operations, shared between the partition readers.
 *
 * @author zinal
 */
public class YdbScanTableOptions implements Serializable,
        ScanBuilder, SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit {
    private static final long serialVersionUID = 4200327347220814437L;

    private final YdbTable table;
    private final YdbTypes types;
    private final int queueMaxSize;

    private int rowLimit;
    private StructType readSchema;

    public YdbScanTableOptions(YdbTable table, CaseInsensitiveStringMap options) {
        this.table = table;
        this.types = new YdbTypes(options);

        this.queueMaxSize = LazyReader.readQueueMaxSize(options);
        this.rowLimit = -1;
        this.readSchema = table.schema();
    }

    @Override
    public Scan build() {
        return new YdbScanTable(table, this);
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        return predicates; // all predicates should be re-checked
    }

    @Override
    public Predicate[] pushedPredicates() {
        return new Predicate[]{}; // all predicates should be re-checked
    }

    @Override
    public boolean pushLimit(int limit) {
        this.rowLimit = limit;
        return false;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.readSchema = requiredSchema;
    }

    public YdbTypes getTypes() {
        return types;
    }

    public StructType getReadSchema() {
        return readSchema;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public int getQueueMaxSize() {
        return queueMaxSize;
    }
}
