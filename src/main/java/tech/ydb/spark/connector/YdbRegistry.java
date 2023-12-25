package tech.ydb.spark.connector;

import java.util.Map;
import java.util.HashMap;

/**
 * YDB Connector registry helps to getOrCreate and retrieve the connector instances.
 *
 * @author zinal
 */
public class YdbRegistry {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbRegistry.class);

    private YdbRegistry() {}

    private static final Map<String, YdbConnector> ITEMS = new HashMap<>();

    public static YdbConnector get(String name) {
        YdbConnector yc;
        synchronized(ITEMS) {
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
        synchronized(ITEMS) {
            yc = ITEMS.get(name);
            if (yc==null) {
                yc = new YdbConnector(name, props);
                ITEMS.put(name, yc);
                newval = true;
            }
        }
        LOG.debug("YdbRegistry.getOrCreate(\"{}\") -> {}, {}", name, newval, yc);
        return yc;
    }

    public static YdbConnector getOrCreate(Map<String, String> props) {
        synchronized(ITEMS) {
            for (YdbConnector yc : ITEMS.values()) {
                if (YdbOptions.connectionMatches(yc.getConnectOptions(), props)) {
                    LOG.debug("YdbRegistry.getOrCreate() -> false, {}", yc);
                    return yc;
                }
            }
            int index = ITEMS.size();
            String name;
            do {
                index = index + 1;
                name = "automatic$" + index;
            } while (ITEMS.containsKey(name));
            YdbConnector yc = new YdbConnector(name, props);
            ITEMS.put(name, yc);
            LOG.debug("YdbRegistry.getOrCreate() -> true, {}", yc);
            return yc;
        }
    }

}
