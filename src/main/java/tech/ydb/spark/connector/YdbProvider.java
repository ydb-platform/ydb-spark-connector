package tech.ydb.spark.connector;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *
 * @author zinal
 */
public class YdbProvider implements TableProvider {

    private YdbTable table;

    private YdbTable getOrLoadTable(Map<String, String> props) {
        String tableName = props.get(YdbOptions.YDB_TABLE);
        if (tableName==null) {
            throw new IllegalArgumentException("Missing table name property");
        }

        synchronized(this) {
            if (table!=null) {
                if (tableName.equals(table.name())
                        && YdbOptions.matches(props, table.getConnector().getConnectOptions())) {
                    return table;
                }
            }
        }

        YdbConnector connector = YdbRegistry.create(props);
        // TODO
        return null;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getOrLoadTable(options).schema();
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return getOrLoadTable(options).partitioning();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return getOrLoadTable(properties);
    }

}
