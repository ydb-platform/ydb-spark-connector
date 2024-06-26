package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import tech.ydb.spark.connector.impl.YdbConnector;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
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

    static final Set<TableCapability> CAPABILITIES;

    static {
        final Set<TableCapability> c = new HashSet<>();
        c.add(TableCapability.BATCH_READ);
        c.add(TableCapability.BATCH_WRITE);
        c.add(TableCapability.ACCEPT_ANY_SCHEMA); // allow YDB to check the schema
        CAPABILITIES = Collections.unmodifiableSet(c);
    }

    private final YdbConnector connector;
    private final YdbTypes types;
    private final String logicalName;
    private final String tablePath;
    private final List<TableColumn> columns;
    private final List<String> keyColumns;
    private final ArrayList<YdbFieldType> keyTypes;
    private final ArrayList<YdbKeyRange> partitions;
    private final Map<String, String> properties;
    private StructType schema;

    /**
     * Create table provider for the real YDB table.
     *
     * @param connector YDB connector
     * @param types YDB type convertor
     * @param logicalName Table logical name
     * @param actualPath Table path
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
        this.properties.put(YdbOptions.TABLE_TYPE, "table"); // TODO: columnshard support
        this.properties.put(YdbOptions.TABLE_PATH, tablePath);
        this.properties.put(YdbOptions.PRIMARY_KEY,
                this.keyColumns.stream().collect(Collectors.joining(",")));
        convertPartitioningSettings(td, this.properties);
        LOG.debug("Loaded table {} with {} columns and {} partitions",
                this.tablePath, this.columns.size(), this.partitions.size());
    }

    /**
     * Create table provider for the real YDB table.
     *
     * @param connector YDB connector
     * @param logicalName Table logical name
     * @param actualPath Table path
     * @param td Table description object obtained from YDB
     */
    YdbTable(YdbConnector connector, String logicalName, String tablePath, TableDescription td) {
        this(connector, connector.getDefaultTypes(), logicalName, tablePath, td);
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
        this.properties.put(YdbOptions.TABLE_TYPE, "index");
        this.properties.put(YdbOptions.TABLE_PATH, tablePath);
        this.properties.put(YdbOptions.PRIMARY_KEY,
                this.keyColumns.stream().collect(Collectors.joining(",")));
        LOG.debug("Loaded index {} with {} columns and {} partitions",
                this.tablePath, this.columns.size(), this.partitions.size());
    }

    /**
     * Create table provider for YDB index.
     *
     * @param connector YDB connector
     * @param logicalName Index table logical name
     * @param tablePath Table path for the actual table (not index)
     * @param tdMain Table description object for the actual table
     * @param ix Index information entry
     * @param tdIx Table description object for the index table
     */
    YdbTable(YdbConnector connector, String logicalName, String tablePath,
            TableDescription tdMain, TableIndex ix, TableDescription tdIx) {
        this(connector, connector.getDefaultTypes(), logicalName, tablePath, tdMain, ix, tdIx);
    }

    private static void convertPartitioningSettings(TableDescription td,
            Map<String, String> properties) {
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

    private static Map<String, TableColumn> buildColumnsMap(TableDescription td) {
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
        return CAPABILITIES;
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
        return new YdbWriteBuilder(this, info);
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
        // TODO: implementation
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
        return new YdbRowLevelBuilder();
    }

    final YdbConnector getConnector() {
        return connector;
    }

    final YdbTypes getTypes() {
        return types;
    }

    final String tablePath() {
        return tablePath;
    }

    final List<String> keyColumns() {
        return keyColumns;
    }

    final ArrayList<YdbFieldType> keyTypes() {
        return keyTypes;
    }

    final ArrayList<YdbKeyRange> partitions() {
        return partitions;
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
