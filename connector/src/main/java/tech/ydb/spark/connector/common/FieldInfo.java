package tech.ydb.spark.connector.common;

import java.io.Serializable;

import tech.ydb.table.description.TableColumn;
import tech.ydb.table.values.Type;

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

    public FieldInfo(TableColumn tc) {
        this(tc.getName(), FieldType.fromSdkType(tc.getType()), tc.getType().getKind() == Type.Kind.OPTIONAL);
    }


    public String getName() {
        return name;
    }

    public FieldType getType() {
        return type;
    }

    public Type toYdbType() {
        return type.toSdkType(nullable);
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return name + ":" + type + (nullable ? "?" : "");
    }
}
