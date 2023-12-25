package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.Map;

/**
 * Generic YDB table operation settings.
 *
 * @author mzinal
 */
public abstract class YdbTableOperationOptions implements Serializable {

    // catalog name and connection options to obtain the YdbConnector instance
    private final String catalogName;
    private final Map<String,String> connectOptions;
    // type mapping settings
    private final YdbTypes types;
    // the name and the path of the table to work with
    private final String tableName;
    private final String tablePath;
    // table columns
    private final Map<String, YdbFieldInfo> fields;

    public YdbTableOperationOptions(YdbTable table) {
        this.catalogName = table.getConnector().getCatalogName();
        this.connectOptions = table.getConnector().getConnectOptions();
        this.types = table.getTypes();
        this.tableName = table.name();
        this.tablePath = table.tablePath();
        this.fields = table.makeColumns();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Map<String, String> getConnectOptions() {
        return connectOptions;
    }

    public YdbTypes getTypes() {
        return types;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTablePath() {
        return tablePath;
    }

    public Map<String, YdbFieldInfo> getFields() {
        return fields;
    }

}
