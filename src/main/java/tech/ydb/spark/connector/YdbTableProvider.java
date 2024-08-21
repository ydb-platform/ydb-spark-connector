package tech.ydb.spark.connector;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.spark.connector.impl.YdbConnector;
import tech.ydb.spark.connector.impl.YdbCreateTable;
import tech.ydb.spark.connector.impl.YdbIntrospectTable;

/**
 * YDB table provider. Registered under the "ydb" name via the following file:
 * META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
 *
 * @author zinal
 */
public class YdbTableProvider extends YdbOptions implements TableProvider, DataSourceRegister {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbTableProvider.class);

    private YdbTable table;

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
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getOrLoadTable(options, null).schema();
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return getOrLoadTable(options, null).partitioning();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return getOrLoadTable(properties, schema);
    }

    private YdbTable getOrLoadTable(Map<String, String> props, StructType schema) {
        // Check that table path is provided
        final String inputTable = props.get(DBTABLE);
        if (StringUtils.isEmpty(inputTable)) {
            throw new IllegalArgumentException("Missing table name property");
        }
        YdbIntrospectTable introspection = new YdbIntrospectTable(props, schema);
        String logicalName = introspection.getLogicalName();
        YdbConnector connector = introspection.getConnector();
        synchronized (this) {
            if (table != null) {
                if (Objects.equals(logicalName, table.name())
                        && connector == table.getConnector()) {
                    return table;
                }
            }
        }

        YdbTable retval = introspection.apply();
        if(!retval.isActualTable()) {
            YdbCreateTable action = new YdbCreateTable(
                    introspection.getTablePath(),
                    YdbCreateTable.convert(introspection.getTypes(), schema),
                    props);
            connector.getRetryCtx().supplyStatus(action::createTable)
                    .join()
                    .expectSuccess("Failed to create table " + logicalName);
            retval = introspection.apply();
            if (!retval.isActualTable()) {
                throw new IllegalStateException("Failed to resolve created table " + logicalName);
            }
        }

        synchronized (this) {
            table = retval;
        }
        return retval;
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}
