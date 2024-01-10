package tech.ydb.spark.connector;

import java.io.Serializable;

/**
 * YDB table field information.
 *
 * @author zinal
 */
public class YdbFieldInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final YdbFieldType type;
    private final boolean nullable;

    public YdbFieldInfo(String name, YdbFieldType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public YdbFieldType getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

}
