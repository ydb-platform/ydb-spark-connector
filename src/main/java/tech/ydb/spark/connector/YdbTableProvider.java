package tech.ydb.spark.connector;

import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.impl.YdbConnector;
import tech.ydb.spark.connector.impl.YdbCreateTable;
import tech.ydb.spark.connector.impl.YdbRegistry;

/**
 * YDB table provider. Registered under the "ydb" name via the following file:
 * META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
 *
 * @author zinal
 */
public class YdbTableProvider extends YdbOptions implements TableProvider, DataSourceRegister {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbTableProvider.class);

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
        return new Loader(options).load(false).schema();
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return new Loader(options).load(false).partitioning();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        final Loader loader = new Loader(properties);
        if (schema == null) {
            // No schema provided, so the table must exist.
            return loader.load(false);
        }
        // We have the schema, so the table may need to be created.
        YdbTable table = loader.load(true);
        if (table != null) {
            // Table already exists.
            return table;
        }
        // No such table - creating it.
        final YdbCreateTable action = new YdbCreateTable(loader.tablePath,
                YdbCreateTable.convert(loader.connector.getDefaultTypes(), schema),
                properties);
        loader.connector.getRetryCtx().supplyStatus(session -> action.createTable(session)).join()
                .expectSuccess("Failed to create table: " + loader.inputTable);
        // Trying to load one once again.
        return loader.load(false);
    }

    /**
     * Generate the normalized table identity from the table path structure.
     */
    final class Loader {

        final YdbConnector connector;
        final YdbTypes types;

        final String inputTable;
        final String tablePath;
        final String logicalName;
        final String indexName;

        Loader(Map<String, String> props) {
            this.connector = YdbRegistry.getOrCreate(props);
            this.types = new YdbTypes(props);

            // Check that table path is provided
            this.inputTable = props.get(DBTABLE);
            if (inputTable == null || inputTable.length() == 0) {
                throw new IllegalArgumentException("Missing property: " + DBTABLE);
            }
            final String database = this.connector.getDatabase();

            LOG.debug("Locating table {} in database {}", inputTable, database);

            // Adjust the input path and build the logical name
            String localPath = inputTable;
            String localName;
            String localIxName = null;
            if (localPath.startsWith("/")) {
                if (!localPath.startsWith(database + "/")) {
                    throw new IllegalArgumentException("Database name ["
                            + database + "] must precede the full table name [" + inputTable + "]");
                }
                localName = localPath.substring(database.length() + 2);
                if (localName.length() == 0) {
                    throw new IllegalArgumentException("Missing table name part in path [" + inputTable + "]");
                }
            } else {
                localName = localPath;
                localPath = database + "/" + localName;
            }
            if (localName.endsWith("/indexImplTable")) {
                // Index table special case - to be name-compatible with the catalog.
                String[] parts = localName.split("[/]");
                localIxName = (parts.length > 2) ? parts[parts.length - 2] : null;
                if (localIxName == null || localIxName.length() == 0) {
                    throw new IllegalArgumentException("Illegal index table reference [" + inputTable + "]");
                }
                String tabName = parts[parts.length - 3];
                final StringBuilder sbLogical = new StringBuilder();
                final StringBuilder sbPath = new StringBuilder();
                sbPath.append(database).append("/");
                for (int ix = 0; ix < parts.length - 3; ++ix) {
                    if (sbLogical.length() > 0) {
                        sbPath.append("/");
                        sbLogical.append("/");
                    }
                    final String part = parts[ix];
                    sbPath.append(part);
                    sbLogical.append(part);
                }
                if (sbLogical.length() > 0) {
                    sbPath.append("/");
                    sbLogical.append("/");
                }
                sbPath.append(tabName);
                // Underscores '_' to mimic the results of YdbCatalog.mergeLocal(),
                // which calls YdbCatalog.safeName() effectivly replacing '/' -> '_'.
                sbLogical.append("ix_").append(tabName).append("_").append(localIxName);
                localName = sbLogical.toString();
                localPath = sbPath.toString();
            }
            this.tablePath = localPath;
            this.logicalName = localName;
            this.indexName = localIxName;

            LOG.debug("Table identity: {}, {}, {}", this.logicalName, this.tablePath, this.indexName);
        }

        YdbTable load(boolean allowMissing) {
            Result<YdbTable> retval = YdbTable.lookup(connector, types,
                    tablePath, logicalName, indexName);
            if (!retval.isSuccess()) {
                Status status = retval.getStatus();
                if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                    if (allowMissing) {
                        return null;
                    }
                    status.expectSuccess("Table not found: " + inputTable);
                }
                status.expectSuccess("Failed to locate table " + inputTable);
            }
            return retval.getValue();
        }
    }

}
