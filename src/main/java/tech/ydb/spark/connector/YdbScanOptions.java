package tech.ydb.spark.connector;

import java.io.Serializable;
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
    private Filter[] filters;
    private StructType requiredSchema;

    public YdbScanOptions(YdbTable table) {
        this.catalogName = table.getConnector().getCatalogName();
        this.connectOptions = table.getConnector().getConnectOptions();
        this.tableName = table.name();
        this.schema = table.schema();
        this.keyColumns = table.keyColumns();
    }

    public Filter[] pushFilters(Filter[] filters) {
        this.filters = filters;
        return new Filter[0];
    }

    public Filter[] pushedFilters() {
        return filters;
    }

    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    public StructType readSchema() {
        if (requiredSchema==null)
            return schema;
        return requiredSchema;
    }

    public Filter[] getFilters() {
        return filters;
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

}
