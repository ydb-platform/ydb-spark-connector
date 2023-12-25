package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.table.description.KeyRange;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;

/**
 * YDB table metadata representation for Spark.
 *
 * @author zinal
 */
public class YdbTable implements Table, SupportsRead, SupportsWrite {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbTable.class);

    static final Set<TableCapability> CAPABILITIES;
    static {
        final Set<TableCapability> c = new HashSet<>();
        c.add(TableCapability.BATCH_READ);
        CAPABILITIES = Collections.unmodifiableSet(c);
    }

    private final YdbConnector connector;
    private final YdbTypes types;
    private final String logicalName;
    private final String tablePath;
    private final List<TableColumn> columns;
    private final List<String> keyColumns;
    private final List<YdbFieldType> keyTypes;
    private final List<YdbKeyRange> partitions;
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
        Map<String,TableColumn> cm = buildColumnsMap(td);
        for (String cname : td.getPrimaryKeys()) {
            TableColumn tc = cm.get(cname);
            this.keyTypes.add(YdbFieldType.fromSdkType(tc.getType()));
        }
        if (td.getKeyRanges()!=null) {
            for (KeyRange kr : td.getKeyRanges()) {
                partitions.add(new YdbKeyRange(kr, types));
            }
        }
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
     * @param td Table description object for the actual table
     * @param ix Index information entry
     * @param td_ix Table description object for the index table
     */
    YdbTable(YdbConnector connector, YdbTypes types,
            String logicalName, String tablePath,
            TableDescription td, TableIndex ix, TableDescription td_ix) {
        this.connector = connector;
        this.types = types;
        this.logicalName = logicalName;
        this.tablePath = tablePath + "/" + ix.getName() + "/indexImplTable";
        this.columns = new ArrayList<>();
        this.keyColumns = ix.getColumns();
        this.keyTypes = new ArrayList<>();
        this.partitions = new ArrayList<>();
        HashSet<String> known = new HashSet<>();
        Map<String,TableColumn> cm = buildColumnsMap(td);
        // Add index key columns
        for (String cname : ix.getColumns()) {
            TableColumn tc = cm.get(cname);
            this.columns.add(tc);
            this.keyTypes.add(YdbFieldType.fromSdkType(tc.getType()));
            known.add(cname);
        }
        // Add index extra columns
        if (ix.getDataColumns()!=null) {
            for (String cname : ix.getDataColumns()) {
                TableColumn tc = cm.get(cname);
                this.columns.add(tc);
                known.add(cname);
            }
        }
        // Add main table's PK columns, as they are in the index too.
        for (String cname : td.getPrimaryKeys()) {
            if (known.add(cname)) {
                TableColumn tc = cm.get(cname);
                this.columns.add(tc);
            }
        }
        if (td_ix.getKeyRanges()!=null) {
            for (KeyRange kr : td_ix.getKeyRanges()) {
                partitions.add(new YdbKeyRange(kr, connector.getDefaultTypes()));
            }
        }
        LOG.debug("Loaded index {} with {} columns and {} partitions",
                this.tablePath, this.columns.size(), this.partitions.size());
    }

    /**
     * Create table provider for YDB index.
     *
     * @param connector YDB connector
     * @param logicalName Index table logical name
     * @param tablePath Table path for the actual table (not index)
     * @param td Table description object for the actual table
     * @param ix Index information entry
     * @param td_ix Table description object for the index table
     */
    YdbTable(YdbConnector connector, String logicalName, String tablePath,
            TableDescription td, TableIndex ix, TableDescription td_ix) {
        this(connector, connector.getDefaultTypes(), logicalName, tablePath, td, ix, td_ix);
    }

    private static Map<String, TableColumn> buildColumnsMap(TableDescription td) {
        Map<String,TableColumn> m = new HashMap<>();
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
        if (schema==null) {
            schema = new StructType(mapFields(columns));
        }
        return schema;
    }

    @Override
    public Map<String, String> properties() {
        final Map<String,String> m = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        for (String kc : keyColumns) {
            if (sb.length()>0)
                sb.append(", ");
            sb.append("`").append(kc).append("`");
        }
        m.put("primary_key", sb.toString());
        m.put("table_path", tablePath);
        return m;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Transform[] partitioning() {
        return new Transform[] {
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

    final List<YdbFieldType> keyTypes() {
        return keyTypes;
    }

    final List<YdbKeyRange> partitions() {
        return partitions;
    }

    private StructField[] mapFields(List<TableColumn> columns) {
        final List<StructField> fields = new ArrayList<>();
        for (TableColumn tc : columns) {
            final DataType dataType = types.mapTypeSpark2Ydb(tc.getType());
            if (dataType != null)
                fields.add(mapField(tc, dataType));
        }
        return fields.toArray(new StructField[0]);
    }

    private StructField mapField(TableColumn tc, DataType dataType) {
        return new StructField(tc.getName(), dataType, types.mapNullable(tc.getType()), Metadata.empty());
    }

    public Map<String, YdbFieldInfo> makeColumns() {
        final Map<String, YdbFieldInfo> m = new HashMap<>();
        for (TableColumn tc : columns) {
            m.put(tc.getName(), new YdbFieldInfo(
                    tc.getName(),
                    YdbFieldType.fromSdkType(tc.getType()),
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
