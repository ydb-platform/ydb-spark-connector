package tech.ydb.spark.connector;

import java.util.Map;
import java.util.HashMap;

/**
 *
 * @author zinal
 */
public final class YdbRegistry {

    private YdbRegistry() {}

    private static final Map<String, YdbConnector> items = new HashMap<>();

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

    public static YdbConnector get(String name) {
        synchronized(items) {
            return items.get(name);
        }
    }

}
