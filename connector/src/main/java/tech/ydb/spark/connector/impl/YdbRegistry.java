package tech.ydb.spark.connector.impl;

import java.util.HashMap;
import java.util.Map;

import tech.ydb.spark.connector.YdbOptions;

/**
 * YDB Connector registry helps to getOrCreate and retrieve the connector instances.
 *
 * @author zinal
 */
public class YdbRegistry {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbRegistry.class);
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

    public static YdbConnector getOrCreate(String name, Map<String, String> props) {
        if (name == null) {
            return getOrCreate(props);
        }
        YdbConnector yc;
        boolean newval = false;
        synchronized (ITEMS) {
            yc = ITEMS.get(name);
            if (yc == null) {
                yc = new YdbConnector(name, props);
                ITEMS.put(name, yc);
                newval = true;
            }
        }
        LOG.debug("YdbRegistry.getOrCreate(\"{}\") -> {}, {}", name, newval, yc);
        return yc;
    }

    public static YdbConnector getOrCreate(Map<String, String> props) {
        synchronized (ITEMS) {
            for (YdbConnector yc : ITEMS.values()) {
                if (YdbOptions.optionsMatches(yc.getConnectOptions(), props)) {
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

}
