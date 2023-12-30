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

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbWriteOptions.class);

    private final StructType inputType;
    private final String queryId;
    private final YdbIngestMethod ingestMethod;
    private final Map<String,String> options;
    private final int maxBulkRows;

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
        if (this.options.containsKey(YdbOptions.YDB_METHOD)) {
            this.ingestMethod = YdbIngestMethod.fromString(
                    this.options.get(YdbOptions.YDB_METHOD));
        } else {
            this.ingestMethod = table.getConnector().getDefaultIngestMethod();
        }
        int n;
        try {
            n = Integer.parseInt(this.options.getOrDefault(YdbOptions.YDB_BATCHSIZE, "500"));
        } catch(NumberFormatException nfe) {
            LOG.warn("Illegal input value for option [{}], default of 500 is used instead",
                    YdbOptions.YDB_BATCHSIZE, nfe);
            n = 500;
        }
        this.maxBulkRows = n;
    }

    public String getQueryId() {
        return queryId;
    }

    public YdbIngestMethod getIngestMethod() {
        return ingestMethod;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public StructType getInputType() {
        return inputType;
    }

    public int getMaxBulkRows() {
        return maxBulkRows;
    }

}
