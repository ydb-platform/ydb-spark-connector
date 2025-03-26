package tech.ydb.spark.connector.impl;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.SessionRetryContext;

/**
 * Generate the normalized table identity from the table path structure.
 *
 * @author VVBondarenko
 * @author zinal
 */
public class YdbIntrospectTable {

    private static final Logger LOG = LoggerFactory.getLogger(YdbIntrospectTable.class);

    private final YdbConnector connector;
    private final YdbTypes types;

    private final String inputTable;
    private final String tablePath;
    private final String logicalName;
    private final String indexName;

    public YdbIntrospectTable(CaseInsensitiveStringMap options) {
        this.connector = YdbRegistry.getOrCreate(options);
        this.types = new YdbTypes(options);

        // Check that table path is provided
        this.inputTable = OperationOption.DBTABLE.read(options);
        if (inputTable == null || inputTable.length() == 0) {
            throw new IllegalArgumentException("Missing property: " + OperationOption.DBTABLE);
        }
        final String database = this.connector.getTransport().getDatabase();

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

    public YdbTable load(boolean allowMissing) {
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

    public SessionRetryContext getRetryCtx() {
        return connector.getRetryCtx();
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public YdbTypes getTypes() {
        return types;
    }

    public String getInputTable() {
        return inputTable;
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

}
