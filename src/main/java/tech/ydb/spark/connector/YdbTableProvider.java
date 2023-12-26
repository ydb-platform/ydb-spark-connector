package tech.ydb.spark.connector;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.sources.DataSourceRegister;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.settings.DescribeTableSettings;

/**
 * YDB table provider.
 * Registered under the "ydb" name via the following file:
 *   META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
 * 
 * @author zinal
 */
public class YdbTableProvider extends YdbOptions implements TableProvider, DataSourceRegister {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbTableProvider.class);

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

    private YdbTable getOrLoadTable(Map<String, String> props) {
        // Check that table path is provided
        final String inputTable = props.get(YDB_TABLE);
        if (inputTable==null || inputTable.length()==0) {
            throw new IllegalArgumentException("Missing table name property");
        }
        // Grab the connector and type convertor.
        final YdbConnector connector = YdbRegistry.getOrCreate(props);
        final YdbTypes types = new YdbTypes(props);
        // Adjust the input path and build the logical name
        final TableIdentity ti = new TableIdentity(inputTable, connector.getDatabase());
        // Return the pre-loaded table if one is available
        synchronized(this) {
            if (table!=null) {
                if (ti.logicalName.equals(table.name()) && connector == table.getConnector()) {
                    return table;
                }
            }
        }
        LOG.debug("Table identity: {}, {}, {}, {}",
                ti.logicalName, ti.tablePath, ti.indexName, ti.indexPath);
        YdbTable retval = connector.getRetryCtx().supplyResult(session -> {
            DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(ti.indexName==null); // shard keys for table case
            Result<TableDescription> td_res = session.describeTable(ti.tablePath, dts).join();
            if (! td_res.isSuccess()) {
                LOG.debug("Failed to load table description for {}: {}", ti.tablePath, td_res.getStatus());
                return CompletableFuture.completedFuture(Result.fail(td_res.getStatus()));
            }
            TableDescription td = td_res.getValue();
            if (ti.indexName==null) {
                return CompletableFuture.completedFuture( Result.success(
                        new YdbTable(connector, types, ti.logicalName, ti.tablePath, td)) );
            }
            dts.setIncludeShardKeyBounds(true); // shard keys for index case
            td_res = session.describeTable(ti.indexPath, dts).join();
            if (! td_res.isSuccess()) {
                LOG.debug("Failed to load index description for {}: {}", ti.indexPath, td_res.getStatus());
                return CompletableFuture.completedFuture(Result.fail(td_res.getStatus()));
            }
            for (TableIndex ix : td.getIndexes()) {
                if (ti.indexName.equals(ix.getName())) {
                    TableDescription td_ix = td_res.getValue();
                    return CompletableFuture.completedFuture( Result.success(
                            new YdbTable(connector, types, ti.logicalName, ti.tablePath, td, ix, td_ix)) );
                }
            }
            LOG.debug("Missing index description in the table for {}", ti.indexPath);
            return CompletableFuture.completedFuture(
                    Result.fail(Status.of(StatusCode.SCHEME_ERROR)
                            .withIssues(Issue.of("Path not found", Issue.Severity.ERROR))));
        }).join().getValue();
        synchronized(this) {
            table = retval;
        }
        return retval;
    }

    /**
     * Implementation details class - made public to allow tests.
     */
    public static final class TableIdentity {
        public final String tablePath;
        public final String logicalName;
        public final String indexName;
        public final String indexPath;

        public TableIdentity(String inputTable, String database) {
            if (inputTable==null || inputTable.length()==0) {
                throw new IllegalArgumentException("Missing table name property");
            }
            // Adjust the input path and build the logical name
            String localPath = inputTable;
            String localName;
            String localIxName = null;
            String localIxPath = null;
            if (localPath.startsWith("/")) {
                if (! localPath.startsWith(database + "/")) {
                    throw new IllegalArgumentException("Database name ["
                            + database + "] must precede the full table name [" + inputTable + "]");
                }
                localName = localPath.substring(database.length() + 2);
                if (localName.length()==0) {
                    throw new IllegalArgumentException("Missing table name part in path [" + inputTable + "]");
                }
            } else {
                localName = localPath;
                localPath = database + "/" + localName;
            }
            if (localName.endsWith("/indexImplTable")) {
                // Index table special case - to be name-compatible with the catalog.
                String[] parts = localName.split("[/]");
                localIxName = (parts.length>2) ? parts[parts.length-2] : null;
                if (localIxName==null || localIxName.length()==0) {
                    throw new IllegalArgumentException("Illegal index table reference [" + inputTable + "]");
                }
                localIxPath = localPath;
                String tabName =  parts[parts.length-3];
                final StringBuilder sbLogical = new StringBuilder(), sbPath = new StringBuilder();
                sbPath.append(database).append("/");
                for (int ix=0; ix<parts.length-3; ++ix) {
                    if (sbLogical.length()>0) {
                        sbPath.append("/");
                        sbLogical.append("/");
                    }
                    final String part = parts[ix];
                    sbPath.append(part);
                    sbLogical.append(part);
                }
                if (sbLogical.length()>0) {
                    sbPath.append("/");
                    sbLogical.append("/");
                }
                sbPath.append(tabName);
                sbLogical.append("ix_").append(tabName).append("_").append(localIxName);
                localName = sbLogical.toString();
                localPath = sbPath.toString();
            }
            this.tablePath = localPath;
            this.logicalName = localName;
            this.indexName = localIxName;
            this.indexPath = localIxPath;
        }
    }

}
