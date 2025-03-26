package tech.ydb.spark.connector;

import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.impl.YdbCreateTable;
import tech.ydb.spark.connector.impl.YdbIntrospectTable;

/**
 * YDB table provider. Registered under the "ydb" name via the following file:
 * META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
 *
 * @author zinal
 */
public class YdbTableProvider implements TableProvider, DataSourceRegister {
    private static final Logger logger = LoggerFactory.getLogger(YdbTableProvider.class);

    /**
     * "Implementations must have a public, 0-arg constructor".
     */
    public YdbTableProvider() {
        logger.debug("created ydb table provider");
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
        logger.info("get inferSchema");
        return new YdbIntrospectTable(options).load(false).schema();
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return new YdbIntrospectTable(options).load(false).partitioning();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        logger.debug("get table");
        final YdbIntrospectTable intro = new YdbIntrospectTable(new CaseInsensitiveStringMap(properties));
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
        final YdbCreateTable action = new YdbCreateTable(
                intro.getTablePath(),
                FieldInfo.fromSchema(intro.getTypes(), schema),
                properties
        );
        intro.getRetryCtx().supplyStatus(session -> action.createTable(session)).join()
                .expectSuccess("Failed to create table: " + intro.getInputTable());
        // Trying to load one once again.
        return intro.load(false);
    }

}
