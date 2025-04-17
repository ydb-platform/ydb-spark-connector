package tech.ydb.spark.connector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import tech.ydb.spark.connector.impl.YdbExecutor;

/**
 * YDB Connector registry helps to getOrCreate and retrieve the connector instances.
 *
 * @author zinal
 */
public class YdbRegistry {
    private static final Map<YdbContext, YdbExecutor> EXECUTORS = new ConcurrentHashMap<>();

    private YdbRegistry() {
    }

    static YdbExecutor getOrCreate(YdbContext ctx, Function<YdbContext, YdbExecutor> mappingFunction) {
        if (EXECUTORS.containsKey(ctx)) {
            return EXECUTORS.get(ctx);
        }
        return EXECUTORS.computeIfAbsent(ctx, mappingFunction);
    }

    public static void closeAll() {
        for (YdbContext key: EXECUTORS.keySet()) {
            YdbExecutor executor = EXECUTORS.remove(key);
            if (executor != null) {
                executor.close();
            }
        }
    }
}
