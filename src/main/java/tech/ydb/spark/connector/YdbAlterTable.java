package tech.ydb.spark.connector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.connector.catalog.TableChange;

import tech.ydb.core.Status;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.PartitioningSettings;

/**
 * Alter Table implementation.
 * 
 * @author zinal
 */
class YdbAlterTable extends YdbPropertyHelper {

    final YdbTypes types;
    final String tablePath;
    final TableDescription td;
    final Set<String> knownNames = new HashSet<>();
    final Map<String, YdbFieldInfo> addColumns = new HashMap<>();
    final Set<String> removeColumns = new HashSet<>();

    YdbAlterTable(YdbConnector connector, String tablePath) {
        super(null);
        this.types = connector.getDefaultTypes();
        this.tablePath = tablePath;
        this.td = connector.getRetryCtx().supplyResult(session -> {
            return session.describeTable(tablePath, new DescribeTableSettings());
        }).join().getValue();
        for (TableColumn tc : this.td.getColumns()) {
            this.knownNames.add(tc.getName());
        }
    }

    void prepare(TableChange.AddColumn change) {
        YdbFieldType yft = types.mapTypeSpark2Ydb(change.dataType());
        if (null == yft) {
            throw new UnsupportedOperationException("Unsupported data type for column: " + change.dataType());
        }
        if (change.fieldNames().length != 1) {
            throw new UnsupportedOperationException("Illegal field name value: "
                    + Arrays.toString(change.fieldNames()));
        }
        final String fieldName = change.fieldNames()[0];
        if (knownNames.contains(fieldName)) {
            throw new UnsupportedOperationException("Column already exists: "
                    + fieldName);
        }
        if (removeColumns.contains(fieldName)) {
            throw new UnsupportedOperationException("Attempt to add and drop the same column: "
                    + fieldName);
        }
        final YdbFieldInfo yfi = new YdbFieldInfo(fieldName, yft, change.isNullable());
        if (addColumns.put(fieldName, yfi) != null) {
            throw new UnsupportedOperationException("Duplicate column add operation: "
                    + fieldName);
        }
    }

    void prepare(TableChange.DeleteColumn change) {
        if (change.fieldNames().length != 1) {
            throw new UnsupportedOperationException("Illegal field name value: "
                    + Arrays.toString(change.fieldNames()));
        }
        final String fieldName = change.fieldNames()[0];
        if (! knownNames.contains(fieldName)) {
            throw new UnsupportedOperationException("Attempt to drop the non-existing column: "
                    + fieldName);
        }
        if (addColumns.containsKey(fieldName)) {
            throw new UnsupportedOperationException("Attempt to add and drop the same column: "
                    + fieldName);
        }
        if (! removeColumns.add(fieldName)) {
            throw new UnsupportedOperationException("Duplicate column drop operation: "
                    + fieldName);
        }
    }

    void prepare(TableChange.SetProperty change) {
        String property = change.property();
        if (! YdbOptions.TABLE_UPDATABLE.contains(property.toUpperCase())) {
            throw new UnsupportedOperationException("Unsupported property for table alteration: "
                    + property);
        }
        properties.put(property.toLowerCase(), change.value());
    }

    void prepare(TableChange.RemoveProperty change) {
        String property = change.property();
        if (! YdbOptions.TABLE_UPDATABLE.contains(property.toUpperCase())) {
            throw new UnsupportedOperationException("Unsupported property for table alteration: "
                    + property);
        }
        properties.put(property.toLowerCase(), "");
    }

    private void applyProperty(String name, String value, PartitioningSettings ps) {
        if (YdbOptions.AP_BY_LOAD.equalsIgnoreCase(name)) {
            if (value==null || value.length()==0) {
                ps.clearPartitioningByLoad();
            } else {
                ps.setPartitioningByLoad(parseBoolean(YdbOptions.AP_BY_LOAD, value));
            }
        } else if (YdbOptions.AP_BY_SIZE.equalsIgnoreCase(name)) {
            if (value==null || value.length()==0) {
                ps.clearPartitioningBySize();
            } else {
                ps.setPartitioningBySize(parseBoolean(YdbOptions.AP_BY_SIZE, value));
            }
        } else if (YdbOptions.AP_PART_SIZE_MB.equalsIgnoreCase(name)) {
            if (value==null || value.length()==0) {
                ps.clearPartitionSize();
            } else {
                ps.setPartitionSize(parseLong(YdbOptions.AP_PART_SIZE_MB, value));
            }
        } else if (YdbOptions.AP_MIN_PARTS.equalsIgnoreCase(name)) {
            if (value==null || value.length()==0) {
                ps.clearMinPartitionsCount();
            } else {
                ps.setMinPartitionsCount(parseLong(YdbOptions.AP_MIN_PARTS, value));
            }
        } else if (YdbOptions.AP_MAX_PARTS.equalsIgnoreCase(name)) {
            if (value==null || value.length()==0) {
                ps.clearMaxPartitionsCount();
            } else {
                ps.setMaxPartitionsCount(parseLong(YdbOptions.AP_MAX_PARTS, value));
            }
        } else {
            throw new IllegalArgumentException("Got unknown property name: " + name);
        }
    }

    CompletableFuture<Status> run(Session session) {
        final AlterTableSettings settings = new AlterTableSettings();
        for (YdbFieldInfo yfi : addColumns.values()) {
            settings.addColumn(yfi.getName(), yfi.getType().toSdkType(yfi.isNullable()));
        }
        for (String name : removeColumns) {
            settings.dropColumn(name);
        }
        if (! properties.isEmpty()) {
            final PartitioningSettings ps = td.getPartitioningSettings();
            for (Map.Entry<String,String> me : properties.entrySet()) {
                applyProperty(me.getKey(), me.getValue(), ps);
            }
            settings.setPartitioningSettings(ps);
        }
        return session.alterTable(tablePath, settings);
    }

}
