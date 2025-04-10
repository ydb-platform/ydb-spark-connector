package tech.ydb.spark.connector.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.TableChange;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.FieldType;
import tech.ydb.spark.connector.common.PartitionOption;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.PartitioningSettings;

/**
 * Alter Table implementation.
 *
 * @author zinal
 */
public class AlterTableBuilder {

    private final YdbTypes types;
    private final TableDescription td;
    private final PartitioningSettings partitionsSettings;

    private final Set<String> knownNames = new HashSet<>();
    private final Map<String, FieldInfo> addColumns = new HashMap<>();
    private final Set<String> removeColumns = new HashSet<>();

    public AlterTableBuilder(YdbTypes types, TableDescription td) {
        this.types = types;
        this.td = td;
        this.partitionsSettings = td.getPartitioningSettings();
        for (TableColumn tc : this.td.getColumns()) {
            this.knownNames.add(tc.getName());
        }
    }

    public void prepare(TableChange.AddColumn change) {
        FieldType yft = types.mapTypeSpark2Ydb(change.dataType());
        if (null == yft) {
            throw new UnsupportedOperationException("Unsupported data type for column: " + change.dataType());
        }
        if (change.fieldNames().length != 1) {
            String fieldName = Arrays.toString(change.fieldNames());
            throw new UnsupportedOperationException("Illegal field name value: " + fieldName);
        }

        final String fieldName = change.fieldNames()[0];
        if (knownNames.contains(fieldName)) {
            throw new UnsupportedOperationException("Column already exists: " + fieldName);
        }
        if (removeColumns.contains(fieldName)) {
            throw new UnsupportedOperationException("Attempt to add and drop the same column: " + fieldName);
        }
        final FieldInfo yfi = new FieldInfo(fieldName, yft, change.isNullable());
        if (addColumns.put(fieldName, yfi) != null) {
            throw new UnsupportedOperationException("Duplicate column add operation: " + fieldName);
        }
    }

    public void prepare(TableChange.DeleteColumn change) {
        if (change.fieldNames().length != 1) {
            String fieldName = Arrays.toString(change.fieldNames());
            throw new UnsupportedOperationException("Illegal field name value: " + fieldName);
        }
        final String fieldName = change.fieldNames()[0];
        if (!knownNames.contains(fieldName)) {
            throw new UnsupportedOperationException("A(ttempt to drop the non-existing column: " + fieldName);
        }
        if (addColumns.containsKey(fieldName)) {
            throw new UnsupportedOperationException("Attempt to add and drop the same column: " + fieldName);
        }
        if (!removeColumns.add(fieldName)) {
            throw new UnsupportedOperationException("Duplicate column drop operation: " + fieldName);
        }
    }

    public void prepare(TableChange.SetProperty change) {
        PartitionOption option = PartitionOption.valueOf(change.property().toUpperCase());
        if (option != null) {
            option.apply(partitionsSettings, change.value());
        } else {
            throw new UnsupportedOperationException("Unsupported property for table alteration: " + change.property());
        }
    }

    public void prepare(TableChange.RemoveProperty change) {
        PartitionOption option = PartitionOption.valueOf(change.property().toUpperCase());
        if (option != null) {
            option.apply(partitionsSettings, null);
        } else {
            throw new UnsupportedOperationException("Unsupported property for table alteration: " + change.property());
        }
    }

    public AlterTableSettings build() {
        AlterTableSettings settings = new AlterTableSettings();
        for (FieldInfo yfi : addColumns.values()) {
            if (yfi.isNullable()) {
                settings.addNullableColumn(yfi.getName(), yfi.getType().toSdkType(false));
            } else {
                settings.addNonnullColumn(yfi.getName(), yfi.getType().toSdkType(false));
            }
        }
        for (String name : removeColumns) {
            settings.dropColumn(name);
        }
        settings.setPartitioningSettings(partitionsSettings);
        return settings;
    }
}
