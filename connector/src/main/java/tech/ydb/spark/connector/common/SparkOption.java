package tech.ydb.spark.connector.common;

import java.util.Map;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface SparkOption {
    String getCode();

    default String read(Map<String, String> options) {
        return options.get(getCode());
    }

    default String read(Map<String, String> options, String defvalue) {
        return options.getOrDefault(getCode(), defvalue);
    }

    default boolean readBoolean(Map<String, String> options, boolean defvalue) {
        if (!options.containsKey(getCode())) {
            return defvalue;
        }
        return parseBoolean(getCode(), options.get(getCode()));
    }

    default int readInt(Map<String, String> options, int defvalue) {
        if (!options.containsKey(getCode())) {
            return defvalue;
        }
        return parseInt(getCode(), options.get(getCode()));
    }

    default <T extends Enum<T>> T readEnum(Map<String, String> options, T defvalue) {
        if (!options.containsKey(getCode())) {
            return defvalue;
        }
        return parseEnum(getCode(), options.get(getCode()), defvalue.getDeclaringClass());
    }

    default void write(Map<String, String> options, String value) {
        options.put(getCode(), value);
    }


    static boolean parseBoolean(String name, String value) {
        if ("1".equalsIgnoreCase(value)
                || "yes".equalsIgnoreCase(value)
                || "true".equalsIgnoreCase(value)
                || "enabled".equalsIgnoreCase(value)) {
            return true;
        }
        if ("0".equalsIgnoreCase(value)
                || "no".equalsIgnoreCase(value)
                || "false".equalsIgnoreCase(value)
                || "disabled".equalsIgnoreCase(value)) {
            return false;
        }

        throw new IllegalArgumentException("Illegal value [" + value + "] for property " + name);
    }

    static long parseLong(String name, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Illegal value [" + value + "] for property " + name, nfe);
        }
    }

    static int parseInt(String name, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Illegal value [" + value + "] for property " + name, nfe);
        }
    }

    static <T extends Enum<T>> T parseEnum(String name, String value, Class<T> clazz) {
        try {
            return Enum.valueOf(clazz, value.toUpperCase());
        } catch (IllegalArgumentException nfe) {
            throw new IllegalArgumentException("Illegal value [" + value + "] for property " + name, nfe);
        }
    }
}
