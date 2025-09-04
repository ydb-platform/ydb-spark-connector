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
    private final Type type;
    private final boolean nullable;

    public FieldInfo(String name, Type type) {
        this.name = name;
        this.nullable = (type.getKind() == Type.Kind.OPTIONAL);
        this.type = type;
    }

    public FieldInfo(TableColumn tc) {
        this.name = tc.getName();
        this.nullable = (tc.getType().getKind() == Type.Kind.OPTIONAL);
        this.type = tc.getType();
    }


    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public Type getSafeType() {
        if (Type.Kind.OPTIONAL == type.getKind()) {
            return type.unwrapOptional();
        }
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return name + ":" + type;
    }
}
