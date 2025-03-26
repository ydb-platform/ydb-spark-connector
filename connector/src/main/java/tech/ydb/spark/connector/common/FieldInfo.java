package tech.ydb.spark.connector.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import tech.ydb.spark.connector.impl.YdbTypes;

/**
 * YDB table field information.
 *
 * @author zinal
 */
public class FieldInfo implements Serializable {
    private static final long serialVersionUID = -4901495186649569832L;

    private final String name;
    private final FieldType type;
    private final boolean nullable;

    public FieldInfo(String name, FieldType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public FieldType getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return name + ":" + type + (nullable ? "?" : "");
    }

    public static FieldInfo fromSchema(YdbTypes types, StructField field) {
        FieldType type = types.mapTypeSpark2Ydb(field.dataType());
        if (type == null) {
            throw new IllegalArgumentException("Unsupported type for table column: " + field.dataType());
        }
        return new FieldInfo(field.name(), type, field.nullable());
    }

    public static List<FieldInfo> fromSchema(YdbTypes types, StructType schema) {
        final List<FieldInfo> fields = new ArrayList<>(schema.size());
        for (StructField field : schema.fields()) {
            FieldType type = types.mapTypeSpark2Ydb(field.dataType());
            if (type == null) {
                throw new IllegalArgumentException("Unsupported type for table column: " + field.dataType());
            }
            fields.add(new FieldInfo(field.name(), type, field.nullable()));
        }
        return fields;
    }

}
