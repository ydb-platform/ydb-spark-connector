package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author zinal
 */
public class YdbScanOptions implements Serializable {

    private final String catalogName;
    private final Map<String,String> connectOptions;
    private final String tableName;
    private final StructType schema;
    private final List<String> keyColumns;
    private final ArrayList<Object> rangeBegin;
    private final ArrayList<Object> rangeEnd;
    private StructType requiredSchema;

    public YdbScanOptions(YdbTable table) {
        this.catalogName = table.getConnector().getCatalogName();
        this.connectOptions = table.getConnector().getConnectOptions();
        this.tableName = table.name();
        this.schema = table.schema();
        this.keyColumns = table.keyColumns();
        this.rangeBegin = new ArrayList<>();
        this.rangeEnd = new ArrayList<>();
    }

    public Filter[] pushFilters(Filter[] filters) {
        if (filters==null) {
            return new Filter[0];
        }
        detectRange(expandFilters(filters));
        return filters;
    }

    public Filter[] pushedFilters() {
        return new Filter[0];
    }

    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    public StructType readSchema() {
        if (requiredSchema==null)
            return schema;
        return requiredSchema;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Map<String, String> getConnectOptions() {
        return connectOptions;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public List<Object> getRangeBegin() {
        return rangeBegin;
    }

    public List<Object> getRangeEnd() {
        return rangeEnd;
    }

    private List<Filter> expandFilters(Filter[] filters) {
        final List<Filter> retval = new ArrayList<>();
        for (Filter f : filters) {
            expandFilter(f, retval);
        }
        return retval;
    }

    private void expandFilter(Filter f, List<Filter> retval) {
        if (f instanceof org.apache.spark.sql.sources.And) {
            org.apache.spark.sql.sources.And fand = (org.apache.spark.sql.sources.And) f;
            expandFilter(fand.left(), retval);
            expandFilter(fand.right(), retval);
        } else {
            retval.add(f);
        }
    }

    /**
     * Very basic filter-to-range conversion logic.
     * Currently covers N equality conditions + 1 optional following range condition.
     * Does NOT handle complex cases like N-dimensional ranges.
     * @param filters input list of filters
     */
    private void detectRange(List<Filter> filters) {
        rangeBegin.clear();
        rangeEnd.clear();
        for (String x : keyColumns) {
            rangeBegin.add(null);
            rangeEnd.add(null);
        }
        for (int pos = 0; pos<keyColumns.size(); ++pos) {
            final String keyColumn = keyColumns.get(pos);
            boolean hasEquality = false;
            for (Filter f : filters) {
                if (f instanceof org.apache.spark.sql.sources.EqualTo) {
                    org.apache.spark.sql.sources.EqualTo x =
                            (org.apache.spark.sql.sources.EqualTo) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeBegin.set(pos, x.value());
                        rangeEnd.set(pos, x.value());
                        hasEquality = true;
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.EqualNullSafe) {
                    org.apache.spark.sql.sources.EqualNullSafe x =
                            (org.apache.spark.sql.sources.EqualNullSafe) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeBegin.set(pos, x.value());
                        rangeEnd.set(pos, x.value());
                        hasEquality = true;
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.GreaterThan) {
                    org.apache.spark.sql.sources.GreaterThan x =
                            (org.apache.spark.sql.sources.GreaterThan) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeBegin.set(pos, x.value());
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.GreaterThanOrEqual) {
                    org.apache.spark.sql.sources.GreaterThanOrEqual x =
                            (org.apache.spark.sql.sources.GreaterThanOrEqual) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeBegin.set(pos, x.value());
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.LessThan) {
                    org.apache.spark.sql.sources.LessThan x =
                            (org.apache.spark.sql.sources.LessThan) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeEnd.set(pos, x.value());
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.LessThanOrEqual) {
                    org.apache.spark.sql.sources.LessThanOrEqual x =
                            (org.apache.spark.sql.sources.LessThanOrEqual) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeEnd.set(pos, x.value());
                        break;
                    }
                }
            } // for (Filter f : ...)
            if (! hasEquality)
                break;
        }
        // Drop trailing nulls
        for (int pos = keyColumns.size()-1; pos>=0; --pos) {
            if (rangeBegin.get(pos)==null && rangeEnd.get(pos)==null) {
                rangeBegin.remove(pos);
                rangeEnd.remove(pos);
            } else {
                break;
            }
        }
    }

}
