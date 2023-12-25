package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * All settings for YDB upsert operations, shared between partition writers.
 *
 * @author zinal
 */
public class YdbUpsertOptions extends YdbTableOperationOptions implements Serializable {

    private final String queryId;
    private final Map<String,String> options;

    public YdbUpsertOptions(YdbTable table, String queryId, Map<String,String> options) {
        super(table);
        this.queryId = queryId;
        this.options = new HashMap<>();
        if (options != null) {
            for (Map.Entry<String,String> me : options.entrySet()) {
                this.options.put(me.getKey().toLowerCase(), me.getValue());
            }
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public Map<String, String> getOptions() {
        return options;
    }

}
