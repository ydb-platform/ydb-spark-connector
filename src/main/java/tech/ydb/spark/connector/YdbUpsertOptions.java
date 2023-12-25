package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.Map;

/**
 * All settings for YDB upsert operations, shared between partition writers.
 *
 * @author zinal
 */
public class YdbUpsertOptions extends YdbTableOperationOptions implements Serializable {

    private final String queryId;

    public YdbUpsertOptions(YdbTable table, String queryId, Map<String,String> options) {
        super(table);
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }

}
