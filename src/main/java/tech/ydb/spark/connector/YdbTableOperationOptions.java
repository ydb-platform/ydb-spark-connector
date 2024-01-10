package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic YDB table operation settings.
 *
 * @author mzinal
 */
public abstract class YdbTableOperationOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    // catalog name and connection options to obtain the YdbConnector instance
    private final String catalogName;
    private final HashMap<String, String> connectOptions;
    // type mapping settings
    private final YdbTypes types;
    // the name and the path of the table to work with
    private final String tableName;
    private final String tablePath;
    // table columns
    private final ArrayList<YdbFieldInfo> fieldsList;
    private final HashMap<String, YdbFieldInfo> fieldsMap;

    public YdbTableOperationOptions(YdbTable table) {
        this.catalogName = table.getConnector().getCatalogName();
        this.connectOptions = new HashMap<>(table.getConnector().getConnectOptions());
        this.types = table.getTypes();
        this.tableName = table.name();
        this.tablePath = table.tablePath();
        this.fieldsList = table.makeColumns();
        this.fieldsMap = new HashMap<>();
        for (YdbFieldInfo yfi : fieldsList) {
            this.fieldsMap.put(yfi.getName(), yfi);
        }
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

    public List<YdbFieldInfo> getFieldsList() {
        return fieldsList;
    }

    public Map<String, YdbFieldInfo> getFieldsMap() {
        return fieldsMap;
    }

}
