package tech.ydb.spark.connector;

import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.spark.connector.impl.YdbCreateTable;
import tech.ydb.spark.connector.impl.YdbIntrospectTable;

/**
 * YDB table provider. Registered under the "ydb" name via the following file:
 * META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
 *
 * @author zinal
 */
public class YdbTableProvider extends YdbOptions implements TableProvider, DataSourceRegister {

    /**
     * "Implementations must have a public, 0-arg constructor".
     */
    public YdbTableProvider() {
    }

    @Override
    public String shortName() {
        return "ydb";
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return new YdbIntrospectTable(options).load(false).schema();
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return new YdbIntrospectTable(options).load(false).partitioning();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        final YdbIntrospectTable intro = new YdbIntrospectTable(properties);
        if (schema == null) {
            // No schema provided, so the table must exist.
            return intro.load(false);
        }
        // We have the schema, so the table may need to be created.
        YdbTable table = intro.load(true);
        if (table != null) {
            // Table already exists.
            return table;
        }
        // No such table - creating it.
        final YdbCreateTable action = new YdbCreateTable(intro.getTablePath(),
                YdbCreateTable.convert(intro.getTypes(), schema),
                properties);
        intro.getRetryCtx().supplyStatus(session -> action.createTable(session)).join()
                .expectSuccess("Failed to create table: " + intro.getInputTable());
        // Trying to load one once again.
        return intro.load(false);
    }

}
