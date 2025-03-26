package tech.ydb.spark.connector.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.common.ConnectionOption;

/**
 * YDB Connector registry helps to getOrCreate and retrieve the connector instances.
 *
 * @author zinal
 */
public class YdbRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(YdbRegistry.class);
    private static final Map<String, YdbConnector> ITEMS = new HashMap<>();

    private YdbRegistry() {
    }

    public static YdbConnector get(String name) {
        YdbConnector yc;
        synchronized (ITEMS) {
            yc = ITEMS.get(name);
        }
        LOG.debug("YdbRegistry.get(\"{}\") -> {}", name, yc);
        return yc;
    }

    public static YdbConnector getOrCreate(String name, CaseInsensitiveStringMap options) {
        if (name == null) {
            return getOrCreate(options);
        }
        YdbConnector yc;
        boolean newval = false;
        synchronized (ITEMS) {
            yc = ITEMS.get(name);
            if (yc == null) {
                yc = new YdbConnector(name, options);
                ITEMS.put(name, yc);
                newval = true;
            }
        }
        LOG.debug("YdbRegistry.getOrCreate(\"{}\") -> {}, {}", name, newval, yc);
        return yc;
    }

    public static YdbConnector getOrCreate(CaseInsensitiveStringMap props) {
        synchronized (ITEMS) {
            for (YdbConnector yc : ITEMS.values()) {
                if (isSameConnection(yc.getOptions(), props)) {
                    LOG.debug("YdbRegistry.getOrCreate() -> false, {}", yc);
                    return yc;
                }
            }
            int index = ITEMS.size();
            String name;
            do {
                index = index + 1;
                name = "ydb$auto$" + index;
            } while (ITEMS.containsKey(name));
            YdbConnector yc = new YdbConnector(name, props);
            ITEMS.put(name, yc);
            LOG.debug("YdbRegistry.getOrCreate() -> true, {}", yc);
            return yc;
        }
    }

    public static boolean isSameConnection(CaseInsensitiveStringMap existing, CaseInsensitiveStringMap referenced) {
        for (ConnectionOption option : ConnectionOption.values()) {
            String v1 = option.read(existing);
            String v2 = option.read(referenced);
            if (!Objects.equals(v1, v2)) {
                return false;
            }
        }
        return true;
    }
}
