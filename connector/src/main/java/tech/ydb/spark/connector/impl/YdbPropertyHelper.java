package tech.ydb.spark.connector.impl;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author zinal
 */
public class YdbPropertyHelper {

    final Map<String, String> properties;

    YdbPropertyHelper(Map<String, String> properties) {
        this.properties = new HashMap<>();
        if (properties != null) {
            for (Map.Entry<String, String> me : properties.entrySet()) {
                this.properties.put(me.getKey().toLowerCase(), me.getValue());
            }
        }
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
        throw new IllegalArgumentException("Illegal value [" + value + "] "
                + "for property " + name);
    }

    boolean getBooleanOption(String name, boolean defval) {
        name = name.trim().toLowerCase();
        String value = properties.get(name);
        if (value != null) {
            value = value.trim();
            if (value.length() > 0) {
                return parseBoolean(name, value);
            }
        }
        return defval;
    }

    static long parseLong(String name, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Illegal value [" + value + "] "
                    + "for property " + name, nfe);
        }
    }

    long getLongOption(String name, long defval) {
        name = name.trim().toLowerCase();
        String value = properties.get(name);
        if (value != null) {
            value = value.trim();
            if (value.length() > 0) {
                return parseLong(name, value);
            }
        }
        return defval;
    }

}
