package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.NotSupportedException;

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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.FieldType;
import tech.ydb.spark.connector.common.KeysRange;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.spark.connector.common.PartitionOption;
import tech.ydb.spark.connector.read.YdbReadTableOptions;
import tech.ydb.spark.connector.write.YdbRowLevelBuilder;
import tech.ydb.spark.connector.write.YdbWrite;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.PartitioningSettings;

/**
 * YDB table metadata representation for Spark.
 *
 * @author zinal
 */
public class YdbTable implements Serializable, Table, SupportsRead, SupportsWrite, SupportsDelete,
        SupportsRowLevelOperations {

    private static final long serialVersionUID = -7889575077751922731L;

    private static final Logger logger = LoggerFactory.getLogger(YdbTable.class);

    public static final String INDEX_TABLE_NAME = "/indexImplTable";

    public enum Type {
        ROW,
        COLUMN,
        INDEX,
    }

    private final YdbContext ctx;

    private final String name;
    private final String path;
    private final Type type;

    private final StructType schema;
    private final FieldInfo[] columns;
    private final FieldInfo[] keyColumns;
    private final KeysRange[] partitions;

    private final HashMap<String, String> properties;

    public YdbTable(YdbContext ctx, YdbTypes types, String name, String path, TableDescription td) {
        this.ctx = ctx;
        this.name = name;
        this.path = path;

        this.type = mapType(path, td);
        this.schema = types.toSparkSchema(td.getColumns());

        this.columns = new FieldInfo[td.getColumns().size()];
        Map<String, FieldInfo> columnsByName = new HashMap<>();
        int idx = 0;
        for (TableColumn column: td.getColumns()) {
            FieldInfo field = new FieldInfo(column);
            this.columns[idx++] = field;
            columnsByName.put(column.getName(), field);
        }

        this.keyColumns = new FieldInfo[td.getPrimaryKeys().size()];
        idx = 0;
        for (String keyColumn: td.getPrimaryKeys()) {
            keyColumns[idx++] = columnsByName.get(keyColumn);
        }

        this.partitions = parsePartitions(types, this.path, td.getKeyRanges());

        this.properties = new HashMap<>();
        OperationOption.TABLE_PATH.write(properties, path);
        OperationOption.TABLE_TYPE.write(properties, type.name());
        OperationOption.PRIMARY_KEY.write(properties, td.getPrimaryKeys().stream().collect(Collectors.joining(",")));
        PartitioningSettings ps = td.getPartitioningSettings();
        if (ps != null) {
            PartitionOption.writeAll(properties, ps);
        }
        logger.debug("Loaded table {} with {} columns and {} partitions", this.path, schema.size(), partitions.length);
    }

    private static KeysRange[] parsePartitions(YdbTypes types, String tablePath, List<KeyRange> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return new KeysRange[0];
        }

        KeysRange[] result = new KeysRange[ranges.size()];
        int idx = 0;
        for (KeyRange kr : ranges) {
            KeysRange parsed = new KeysRange(kr, types);
            if (parsed.isUnrestricted()) {
                if (result.length > 1) {
                    logger.warn("Unrestricted partition for table {}, ignoring partition metadata", tablePath);
                }
                return new KeysRange[] {parsed};
            }
            result[idx++] = new KeysRange(kr, types);
        }

        return result;
    }

    @Override
    public String name() {
        return name;
    }

    public String getTablePath() {
        return path;
    }

    public Type getType() {
        return type;
    }

    public YdbContext getCtx() {
        return ctx;
    }

    public FieldInfo[] getKeyColumns() {
        return keyColumns;
    }

    public FieldInfo[] getAllColumns() {
        return columns;
    }

    public KeysRange[] getPartitions() {
        return partitions;
    }


    @Override
    public String toString() {
        return "YdbTable{name=" + name + ", path='" + path + "', ctx=" + ctx + "}";
    }

    @Override
    @SuppressWarnings("deprecation")
    public StructType schema() {
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

        switch (type) {
            case ROW:
                c.add(TableCapability.BATCH_WRITE);
                c.add(TableCapability.TRUNCATE);
                break;
            case COLUMN:
                c.add(TableCapability.BATCH_WRITE);
                break;
            case INDEX:
            default:
                break;
        }

        return c;
    }

    @Override
    public Transform[] partitioning() {
        final String[] cols = new String[keyColumns.length];
        int idx = 0;
        for (FieldInfo key : keyColumns) {
            cols[idx++] = key.getName();
        }

        return new Transform[] {
            Expressions.bucket(partitions.length, cols)
        };
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        switch (type) {
            case COLUMN:
                throw new NotSupportedException("Column name are not supported");
            case ROW:
            case INDEX:
            default:
                return new YdbReadTableOptions(this, options);
        }
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        boolean truncate = OperationOption.TRUNCATE.readBoolean(info.options(), false);
        logger.debug("Creating YdbWriteBuilder for table {} with truncate={}", path, truncate);
        return new YdbWrite(this, info, truncate);
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
        return ctx.getExecutor().truncateTable(path);
    }

    @Override
    public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
        return new YdbRowLevelBuilder();
    }

    private static Type mapType(String tablePath, TableDescription td) {
        if (tablePath.endsWith(INDEX_TABLE_NAME)) {
            return Type.INDEX;
        }

        switch (td.getStoreType()) {
            case COLUMN:
                return Type.COLUMN;
            case ROW:
                return Type.ROW;
            default:
                throw new IllegalArgumentException("Uknown table store type " + td.getStoreType());
        }
    }

    public static TableDescription buildTableDesctiption(List<FieldInfo> fields, CaseInsensitiveStringMap options) {
        Set<String> fieldNames = fields.stream().map(FieldInfo::getName).collect(Collectors.toSet());
        String userPrimaryKeys = OperationOption.PRIMARY_KEY.read(options, "");

        List<String> primaryKeys = new ArrayList<>();
        for (String keyName : userPrimaryKeys.split(",")) {
            String trimmed = keyName.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (!fieldNames.contains(trimmed)) {
                throw new IllegalArgumentException("Specified column " + trimmed + " not found in source schema");
            }
            primaryKeys.add(trimmed);
        }

        if (primaryKeys.isEmpty()) {
            String autoPkName = OperationOption.AUTO_PK.read(options, OperationOption.DEFAULT_AUTO_PK);
            fields.add(new FieldInfo(autoPkName, FieldType.Text, false));
            primaryKeys.add(autoPkName);
        }

        TableDescription.Builder tdb = TableDescription.newBuilder();
        for (FieldInfo field : fields) {
            if (field.isNullable()) {
                tdb.addNullableColumn(field.getName(), field.getType().toSdkType());
            } else {
                tdb.addNonnullColumn(field.getName(), field.getType().toSdkType());
            }
        }
        tdb.setPrimaryKeys(primaryKeys);

        PartitioningSettings partitioning = new PartitioningSettings();
        PartitionOption.writeAll(options, partitioning);
        tdb.setPartitioningSettings(partitioning);

        return tdb.build();
    }
}
