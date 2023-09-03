package tech.ydb.spark.connector;

import java.util.Map;
import java.util.HashMap;

/**
 * YDB Connector registry helps to create and retrieve the connector instances.
 *
 * @author zinal
 */
final class YdbRegistry {

    private YdbRegistry() {}

    private static final Map<String, YdbConnector> items = new HashMap<>();

    public static YdbConnector get(String name) {
        synchronized(items) {
            return items.get(name);
        }
    }

    public static YdbConnector create(String name, Map<String, String> props) {
        synchronized(items) {
            YdbConnector yc = items.get(name);
            if (yc==null) {
                yc = new YdbConnector(name, props);
                items.put(name, yc);
            }
            return yc;
        }
    }

    public static YdbConnector create(Map<String, String> props) {
        synchronized(items) {
            for (YdbConnector yc : items.values()) {
                if (YdbOptions.matches(yc.getConnectOptions(), props))
                    return yc;
            }
            int index = items.size();
            String name;
            do {
                index = index + 1;
                name = "automatic$" + index;
            } while (items.containsKey(name));
            YdbConnector yc = new YdbConnector(name, props);
            items.put(name, yc);
            return yc;
        }
    }

}
