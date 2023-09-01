package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;

/**
 *
 * @author mzinal
 */
public class YdbTable implements Table, SupportsRead {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbTable.class);

    static final Set<TableCapability> CAPABILITIES;
    static {
        final Set<TableCapability> c = new HashSet<>();
        c.add(TableCapability.BATCH_READ);
        CAPABILITIES = Collections.unmodifiableSet(c);
    }

    private final YdbConnector connector;
    private final String logicalName;
    private final String physicalName;
    private final List<TableColumn> columns;
    private final List<String> keyColumns;
    private final List<YdbFieldType> keyTypes;
    private final List<YdbKeyRange> partitions;
    private StructType schema;

    YdbTable(YdbConnector connector, String logicalName, String physicalName, TableDescription td) {
        this.connector = connector;
        this.logicalName = logicalName;
        this.physicalName = physicalName;
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
                partitions.add(new YdbKeyRange(kr));
            }
        }
        LOG.debug("Loaded table {} with {} columns and {} partitions",
                this.physicalName, this.columns.size(), this.partitions.size());
    }

    YdbTable(YdbConnector connector, String logicalName, String physicalName,
            TableDescription td, TableIndex ix, TableDescription td_ix) {
        this.connector = connector;
        this.logicalName = logicalName;
        this.physicalName = physicalName + "/" + ix.getName() + "/indexImplTable";
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
                partitions.add(new YdbKeyRange(kr));
            }
        }
        LOG.debug("Loaded index {} with {} columns and {} partitions",
                this.physicalName, this.columns.size(), this.partitions.size());
    }

    private static Map<String, TableColumn> buildColumnsMap(TableDescription td) {
        Map<String,TableColumn> m = new HashMap<>();
        for (TableColumn tc : td.getColumns()) {
            m.put(tc.getName(), tc);
        }
        return m;
    }

    final YdbConnector getConnector() {
        return connector;
    }

    @Override
    public String name() {
        return logicalName;
    }

    public String tablePath() {
        return physicalName;
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
        // TODO: return primary key + storage characteristics
        return Collections.emptyMap();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    private StructField[] mapFields(List<TableColumn> columns) {
        final List<StructField> fields = new ArrayList<>();
        for (TableColumn tc : columns) {
            final DataType dataType = YdbTypes.mapType(tc.getType());
            if (dataType != null)
                fields.add(mapField(tc, dataType));
        }
        return fields.toArray(new StructField[0]);
    }

    private StructField mapField(TableColumn tc, DataType dataType) {
        // TODO: mapping dictionary support (specifically for dates).
        return new StructField(tc.getName(), dataType, YdbTypes.mapNullable(tc.getType()), Metadata.empty());
    }

    @Override
    public Transform[] partitioning() {
        return keyColumns.stream().map(v -> Expressions.identity(v)).toArray(Transform[]::new);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new YdbScanBuilder(this);
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

    @Override
    public String toString() {
        return "YdbTable{" + "connector=" + connector.getCatalogName()
                + ", physicalName=" + physicalName + '}';
    }

}
