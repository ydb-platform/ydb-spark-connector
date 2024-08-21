package tech.ydb.spark.connector.impl;

import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.settings.DescribeTableSettings;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static tech.ydb.spark.connector.YdbOptions.DBTABLE;

public class YdbIntrospectTable {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbIntrospectTable.class);

    private final Map<String, String> props;
    private final StructType schema;

    private final YdbConnector connector;
    private final YdbTypes types;

    private String tablePath;
    private String logicalName;
    private String indexName;
    private String indexPath;


    public YdbIntrospectTable(Map<String, String> props) {
        this(props, null);
    }

    public YdbIntrospectTable(Map<String, String> props, StructType schema) {
        this.props = props;
        this.schema = schema;
        connector = YdbRegistry.getOrCreate(props);
        types = new YdbTypes(props);

        resolveIdentity(props.get(DBTABLE), connector.getDatabase());
    }

    private void resolveIdentity(String inputTable, String database) {
        if (inputTable == null || inputTable.length() == 0) {
            throw new IllegalArgumentException("Missing table name property");
        }
        // Adjust the input path and build the logical name
        String localPath = inputTable;
        String localName;
        String localIxName = null;
        String localIxPath = null;
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
            localIxPath = localPath;
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
            sbLogical.append("ix_").append(tabName).append("_").append(localIxName);
            localName = sbLogical.toString();
            localPath = sbPath.toString();
        }
        this.tablePath = localPath;
        this.logicalName = localName;
        this.indexName = localIxName;
        this.indexPath = localIxPath;

        LOG.debug("Table identity: {}, {}, {}, {}", logicalName, tablePath, indexName, indexPath);
    }

    public CompletableFuture<Result<YdbTable>> run(Session session) {
        DescribeTableSettings dts = new DescribeTableSettings();
        dts.setIncludeShardKeyBounds(indexName == null); // shard keys for table case
        Result<TableDescription> tdRes = session.describeTable(tablePath, dts).join();
        if (!tdRes.isSuccess()) {
            if (schema == null) {
                LOG.debug("Failed to load table description for {}: {}", tablePath, tdRes.getStatus());
                return CompletableFuture.completedFuture(Result.fail(tdRes.getStatus()));
            }
            // not such table => create a new, but it's not really related to this action
            return CompletableFuture.completedFuture(Result.success(YdbTable.buildShell()));
        }
        TableDescription td = tdRes.getValue();
        if (indexName == null) {
            return CompletableFuture.completedFuture(Result.success(
                    new YdbTable(connector, types, logicalName, tablePath, td)));
        }
        dts.setIncludeShardKeyBounds(true); // shard keys for index case
        tdRes = session.describeTable(indexPath, dts).join();
        if (!tdRes.isSuccess()) {
            LOG.debug("Failed to load index description for {}: {}", indexPath, tdRes.getStatus());
            return CompletableFuture.completedFuture(Result.fail(tdRes.getStatus()));
        }
        for (TableIndex ix : td.getIndexes()) {
            if (indexName.equals(ix.getName())) {
                TableDescription tdIx = tdRes.getValue();
                return CompletableFuture.completedFuture(Result.success(
                        new YdbTable(connector, types, logicalName, tablePath, td, ix, tdIx)));
            }
        }
        LOG.debug("Missing index description in the table for {}", indexPath);
        return CompletableFuture.completedFuture(
                Result.fail(Status.of(StatusCode.SCHEME_ERROR)
                        .withIssues(Issue.of("Path not found", Issue.Severity.ERROR))));
    }

    public YdbTable apply() {
        return connector.getRetryCtx()
                .supplyResult(this::run)
                .join()
                .getValue();
    }


    public String getTablePath() {
        return tablePath;
    }

    public String getLogicalName() {
        return logicalName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public YdbTypes getTypes() {
        return types;
    }
}
