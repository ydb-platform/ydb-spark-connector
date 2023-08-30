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

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbScanOptions.class);

    private final String catalogName;
    private final Map<String,String> connectOptions;
    private final String tablePath;
    private final String tableName;
    private final StructType schema;
    private final List<String> keyColumns;
    private final List<YdbFieldType> keyTypes;
    private final ArrayList<Object> rangeBegin;
    private final ArrayList<Object> rangeEnd;
    private int rowLimit;
    private StructType requiredSchema;

    public YdbScanOptions(YdbTable table) {
        this.catalogName = table.getConnector().getCatalogName();
        this.connectOptions = table.getConnector().getConnectOptions();
        this.tableName = table.name();
        this.tablePath = table.tablePath();
        this.schema = table.schema();
        this.keyColumns = new ArrayList<>(table.keyColumns()); // ensure serializable list
        this.keyTypes = table.keyTypes();
        this.rangeBegin = new ArrayList<>();
        this.rangeEnd = new ArrayList<>();
        this.rowLimit = -1;
    }

    public Filter[] pushFilters(Filter[] filters) {
        if (filters==null) {
            return new Filter[0];
        }
        detectRangeSimple(flattenFilters(filters));
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

    public String getTablePath() {
        return tablePath;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public List<YdbFieldType> getKeyTypes() {
        return keyTypes;
    }

    public List<Object> getRangeBegin() {
        return rangeBegin;
    }

    public List<Object> getRangeEnd() {
        return rangeEnd;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

    /**
     * Put all filters connected with AND directly into the list of filters, recursively.
     * @param filters Input filters
     * @return Flattened filters
     */
    private List<Filter> flattenFilters(Filter[] filters) {
        final List<Filter> retval = new ArrayList<>();
        for (Filter f : filters) {
            flattenFilter(f, retval);
        }
        return retval;
    }

    /**
     * Put all filters connected with AND directly into the list of filters, recursively.
     * @param f Input filter to be processed
     * @param retval The resulting list of flattened filters
     */
    private void flattenFilter(Filter f, List<Filter> retval) {
        if (f instanceof org.apache.spark.sql.sources.And) {
            org.apache.spark.sql.sources.And fand = (org.apache.spark.sql.sources.And) f;
            flattenFilter(fand.left(), retval);
            flattenFilter(fand.right(), retval);
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
    private void detectRangeSimple(List<Filter> filters) {
        if (filters==null || filters.isEmpty()) {
            return;
        }
        LOG.debug("Calculating scan ranges for filters {}", filters);
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
                        rangeBegin.set(pos, max(rangeBegin.get(pos), x.value()));
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.GreaterThanOrEqual) {
                    org.apache.spark.sql.sources.GreaterThanOrEqual x =
                            (org.apache.spark.sql.sources.GreaterThanOrEqual) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeBegin.set(pos, max(rangeBegin.get(pos), x.value()));
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.LessThan) {
                    org.apache.spark.sql.sources.LessThan x =
                            (org.apache.spark.sql.sources.LessThan) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeEnd.set(pos, min(rangeEnd.get(pos), x.value()));
                        break;
                    }
                } else if (f instanceof org.apache.spark.sql.sources.LessThanOrEqual) {
                    org.apache.spark.sql.sources.LessThanOrEqual x =
                            (org.apache.spark.sql.sources.LessThanOrEqual) f;
                    if (keyColumn.equals(x.attribute())) {
                        rangeEnd.set(pos, min(rangeEnd.get(pos), x.value()));
                        break;
                    }
                }
            } // for (Filter f : ...)
            if (! hasEquality)
                break;
        }
        // Drop trailing nulls
        while (! rangeBegin.isEmpty()) {
            int pos = rangeBegin.size() - 1;
            if ( rangeBegin.get(pos) == null )
                rangeBegin.remove(pos);
            else
                break;
        }
        while (! rangeEnd.isEmpty()) {
            int pos = rangeEnd.size() - 1;
            if ( rangeEnd.get(pos) == null )
                rangeEnd.remove(pos);
            else
                break;
        }
        LOG.debug("Calculated scan ranges {} -> {}", rangeBegin, rangeEnd);
    }

    private static Object max(Object o1, Object o2) {
        if (o1==null || o1==o2) {
            return o2;
        }
        if (o2==null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable)o2).compareTo(o1) > 0 ? o2 : o1;
        }
        return o2;
    }

    private static Object min(Object o1, Object o2) {
        if (o1==null || o1==o2) {
            return o2;
        }
        if (o2==null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable)o2).compareTo(o1) < 0 ? o2 : o1;
        }
        return o2;
    }

}
