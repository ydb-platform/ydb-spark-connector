package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.types.StructType;

/**
 * All settings for YDB upsert operations, shared between partition writers.
 *
 * @author zinal
 */
public class YdbWriteOptions extends YdbTableOperationOptions implements Serializable {

    private final StructType inputType;
    private final String queryId;
    private final Map<String,String> options;

    public YdbWriteOptions(YdbTable table, StructType inputType,
            String queryId, Map<String,String> options) {
        super(table);
        this.inputType = inputType;
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

    public StructType getInputType() {
        return inputType;
    }

}
