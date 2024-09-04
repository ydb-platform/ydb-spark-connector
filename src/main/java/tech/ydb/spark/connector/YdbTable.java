package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.impl.YdbConnector;
import tech.ydb.spark.connector.impl.YdbTruncateTable;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.PartitioningSettings;

/**
 * YDB table metadata representation for Spark.
 *
 * @author zinal
 */
public class YdbTable implements Table,
        SupportsRead, SupportsWrite, SupportsDelete, SupportsRowLevelOperations {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbTable.class);

    private final YdbConnector connector;
    private final YdbTypes types;
    private final String logicalName;
    private final String tablePath;
    private final List<TableColumn> columns;
    private final List<String> keyColumns;
    private final ArrayList<YdbFieldType> keyTypes;
    private final ArrayList<YdbKeyRange> partitions;
    private final Map<String, String> properties;
    private final YdbStoreType storeType;
    private StructType schema;

    /**
     * Create table provider for the real YDB table.
     *
     * @param connector YDB connector
     * @param types YDB type convertor
     * @param logicalName Table logical name
     * @param tablePath Table path
     * @param td Table description object obtained from YDB
     */
    YdbTable(YdbConnector connector, YdbTypes types,
            String logicalName, String tablePath, TableDescription td) {
        this.connector = connector;
        this.types = types;
        this.logicalName = logicalName;
        this.tablePath = tablePath;
        this.columns = td.getColumns();
        this.keyColumns = td.getPrimaryKeys();
        this.keyTypes = new ArrayList<>();
        this.partitions = new ArrayList<>();
        this.properties = new HashMap<>();
        this.storeType = convertStoreType(td);
        Map<String, TableColumn> cm = buildColumnsMap(td);
        for (String cname : td.getPrimaryKeys()) {
            TableColumn tc = cm.get(cname);
            this.keyTypes.add(YdbFieldType.fromSdkType(tc.getType()));
        }
        if (!connector.isSinglePartitionScans() && td.getKeyRanges() != null) {
            for (KeyRange kr : td.getKeyRanges()) {
                YdbKeyRange ykr = new YdbKeyRange(kr, types);
                if (ykr.isUnrestricted() && !partitions.isEmpty()) {
                    LOG.warn("Unrestricted partition for table {}, ignoring partition metadata",
                            this.tablePath);
                    partitions.clear();
                    break;
                } else {
                    partitions.add(ykr);
                }
            }
        }
        fillProperties(this.properties, this.tablePath, this.storeType, this.keyColumns);
        convertPartitioningSettings(td, this.properties);
        LOG.debug("Loaded table {} with {} columns and {} partitions",
                this.tablePath, this.columns.size(), this.partitions.size());
    }

    /**
     * Create table provider for YDB index.
     *
     * @param connector YDB connector
     * @param types YDB type convertor
     * @param logicalName Index table logical name
     * @param tablePath Table path for the actual table (not index)
     * @param tdMain Table description object for the actual table
     * @param ix Index information entry
     * @param tdIx Table description object for the index table
     */
    YdbTable(YdbConnector connector, YdbTypes types,
            String logicalName, String tablePath,
            TableDescription tdMain, TableIndex ix, TableDescription tdIx) {
        this.connector = connector;
        this.types = types;
        this.logicalName = logicalName;
        this.tablePath = tablePath + "/" + ix.getName() + "/indexImplTable";
        this.columns = new ArrayList<>();
        this.keyColumns = ix.getColumns();
        this.keyTypes = new ArrayList<>();
        this.partitions = new ArrayList<>();
        this.properties = new HashMap<>();
        this.storeType = YdbStoreType.INDEX;
        HashSet<String> known = new HashSet<>();
        Map<String, TableColumn> cm = buildColumnsMap(tdMain);
        // Add index key columns
        for (String cname : ix.getColumns()) {
            TableColumn tc = cm.get(cname);
            this.columns.add(tc);
            this.keyTypes.add(YdbFieldType.fromSdkType(tc.getType()));
            known.add(cname);
        }
        // Add index extra columns
        if (ix.getDataColumns() != null) {
            for (String cname : ix.getDataColumns()) {
                TableColumn tc = cm.get(cname);
                this.columns.add(tc);
                known.add(cname);
            }
        }
        // Add main table's PK columns, as they are in the index too.
        for (String cname : tdMain.getPrimaryKeys()) {
            if (known.add(cname)) {
                TableColumn tc = cm.get(cname);
                this.columns.add(tc);
            }
        }
        if (!connector.isSinglePartitionScans() && tdIx.getKeyRanges() != null) {
            for (KeyRange kr : tdIx.getKeyRanges()) {
                partitions.add(new YdbKeyRange(kr, connector.getDefaultTypes()));
            }
        }
        fillProperties(this.properties, this.tablePath, this.storeType, this.keyColumns);
        LOG.debug("Loaded index {} with {} columns and {} partitions",
                this.tablePath, this.columns.size(), this.partitions.size());
    }

    public static Result<YdbTable> lookup(YdbConnector connector, YdbTypes types,
            String tablePath, String logicalName, String indexName) {
        return connector.getRetryCtx().supplyResult(session -> {
            // Describe the main table
            DescribeTableSettings dts = new DescribeTableSettings();
            // Need key bounds for the main table only
            dts.setIncludeShardKeyBounds(indexName == null);
            Result<TableDescription> tdRes = session.describeTable(tablePath, dts).join();
            if (!tdRes.isSuccess()) {
                return CompletableFuture.completedFuture(Result.fail(tdRes.getStatus()));
            }
            TableDescription td = tdRes.getValue();
            if (indexName == null) {
                // No index name - construct the YdbTable for the main table
                return CompletableFuture.completedFuture(Result.success(
                        new YdbTable(connector, types, logicalName, tablePath, td)));
            }
            for (TableIndex ix : td.getIndexes()) {
                if (indexName.equals(ix.getName())) {
                    // Grab the description for the secondary index table.
                    String indexPath = tablePath + "/" + ix.getName() + "/indexImplTable";
                    dts.setIncludeShardKeyBounds(true);
                    tdRes = session.describeTable(indexPath, dts).join();
                    if (!tdRes.isSuccess()) {
                        return CompletableFuture.completedFuture(Result.fail(tdRes.getStatus()));
                    }
                    TableDescription tdIx = tdRes.getValue();
                    // Construct the YdbTable object for the index
                    return CompletableFuture.completedFuture(Result.success(
                            new YdbTable(connector, types, logicalName, tablePath, td, ix, tdIx)));
                }
            }
            return CompletableFuture.completedFuture(
                    Result.fail(Status.of(StatusCode.SCHEME_ERROR)
                            .withIssues(Issue.of("Path not found", Issue.Severity.ERROR))));
        }).join();
    }

    static YdbStoreType convertStoreType(TableDescription td) {
        switch (td.getStoreType()) {
            case COLUMN:
                return YdbStoreType.COLUMN;
            case ROW:
                return YdbStoreType.ROW;
            default:
                return YdbStoreType.UNSPECIFIED;
        }
    }

    static void fillProperties(Map<String, String> props, String tablePath,
            YdbStoreType storeType, List<String> keyColumns) {
        props.clear();
        props.put(YdbOptions.TABLE_TYPE, storeType.name());
        props.put(YdbOptions.TABLE_PATH, tablePath);
        props.put(YdbOptions.PRIMARY_KEY,
                keyColumns.stream().collect(Collectors.joining(",")));
    }

    static void convertPartitioningSettings(TableDescription td, Map<String, String> properties) {
        PartitioningSettings ps = td.getPartitioningSettings();
        if (ps != null) {
            Boolean bv = ps.getPartitioningBySize();
            if (bv != null) {
                properties.put(YdbOptions.AP_BY_SIZE.toLowerCase(), bv ? "ENABLED" : "DISABLED");
            }
            bv = ps.getPartitioningByLoad();
            if (bv != null) {
                properties.put(YdbOptions.AP_BY_LOAD.toLowerCase(), bv ? "ENABLED" : "DISABLED");
            }
            Long lv = ps.getPartitionSizeMb();
            if (lv != null) {
                properties.put(YdbOptions.AP_PART_SIZE_MB.toLowerCase(), lv.toString());
            }
            lv = ps.getMinPartitionsCount();
            if (lv != null) {
                properties.put(YdbOptions.AP_MIN_PARTS.toLowerCase(), lv.toString());
            }
            lv = ps.getMaxPartitionsCount();
            if (lv != null) {
                properties.put(YdbOptions.AP_MAX_PARTS.toLowerCase(), lv.toString());
            }
        }
    }

    static Map<String, TableColumn> buildColumnsMap(TableDescription td) {
        Map<String, TableColumn> m = new HashMap<>();
        for (TableColumn tc : td.getColumns()) {
            m.put(tc.getName(), tc);
        }
        return m;
    }

    @Override
    public String name() {
        return logicalName;
    }

    @Override
    public StructType schema() {
        if (schema == null) {
            schema = new StructType(mapFields(columns));
        }
        return schema;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public Set<TableCapability> capabilities() {
        final Set<TableCapability> c = new HashSet<>();
        c.add(TableCapability.BATCH_READ);
        c.add(TableCapability.ACCEPT_ANY_SCHEMA); // allow YDB to check the schema
        if (!YdbStoreType.INDEX.equals(storeType)) {
            // tables support writes, while indexes do not
            c.add(TableCapability.BATCH_WRITE);
            c.add(TableCapability.TRUNCATE);
        }
        return c;
    }

    @Override
    public Transform[] partitioning() {
        return new Transform[]{
            Expressions.bucket(partitions.size(), keyColumns.toArray(new String[]{}))
        };
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new YdbScanBuilder(this);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        boolean truncate = info.options().getBoolean(YdbOptions.TRUNCATE, false);
        LOG.debug("Creating YdbWriteBuilder for table {} with truncate={}", tablePath, truncate);
        return new YdbWriteBuilder(this, info, truncate);
    }

    @Override
    public boolean canDeleteWhere(Filter[] filters) {
        // We prefer per-row operations
        return false;
    }

    @Override
    public void deleteWhere(Filter[] filters) {
        // Should not be called, as canDeleteWhere() returns false.
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean truncateTable() {
        final YdbTruncateTable action = new YdbTruncateTable(tablePath);
        connector.getRetryCtx().supplyStatus(session -> action.run(session))
                .join()
                .expectSuccess();
        return true;
    }

    @Override
    public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
        return new YdbRowLevelBuilder();
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public YdbTypes getTypes() {
        return types;
    }

    public String getTablePath() {
        return tablePath;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public ArrayList<YdbFieldType> getKeyTypes() {
        return keyTypes;
    }

    public ArrayList<YdbKeyRange> getPartitions() {
        return partitions;
    }

    public YdbStoreType getStoreType() {
        return storeType;
    }

    private StructField[] mapFields(List<TableColumn> columns) {
        final List<StructField> fields = new ArrayList<>();
        for (TableColumn tc : columns) {
            final DataType dataType = types.mapTypeYdb2Spark(tc.getType());
            if (dataType != null) {
                fields.add(mapField(tc, dataType));
            }
        }
        return fields.toArray(new StructField[0]);
    }

    private StructField mapField(TableColumn tc, DataType dataType) {
        return new StructField(tc.getName(), dataType, types.mapNullable(tc.getType()), Metadata.empty());
    }

    public ArrayList<YdbFieldInfo> makeColumns() {
        final ArrayList<YdbFieldInfo> m = new ArrayList<>();
        for (TableColumn tc : columns) {
            m.add(new YdbFieldInfo(tc.getName(), YdbFieldType.fromSdkType(tc.getType()),
                    tech.ydb.table.values.Type.Kind.OPTIONAL.equals(tc.getType().getKind())
            ));
        }
        return m;
    }

    @Override
    public String toString() {
        return "YdbTable:" + connector.getCatalogName() + ":" + tablePath;
    }

}
