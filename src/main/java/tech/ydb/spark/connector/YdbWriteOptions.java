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

    private static final long serialVersionUID = 1L;

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbWriteOptions.class);

    private final StructType tableType;
    private final StructType inputType;
    private final boolean mapByNames;
    private final String queryId;
    private final YdbIngestMethod ingestMethod;
    private final Map<String, String> options;
    private final int maxBulkRows;

    public YdbWriteOptions(YdbTable table, boolean mapByNames, StructType inputType,
            String queryId, Map<String, String> options) {
        super(table);
        this.tableType = table.schema();
        this.inputType = inputType;
        this.mapByNames = mapByNames;
        this.queryId = queryId;
        this.options = new HashMap<>();
        if (options != null) {
            for (Map.Entry<String, String> me : options.entrySet()) {
                this.options.put(me.getKey().toLowerCase(), me.getValue());
            }
        }
        if (this.options.containsKey(YdbOptions.INGEST_METHOD)) {
            this.ingestMethod = YdbIngestMethod.fromString(
                    this.options.get(YdbOptions.INGEST_METHOD));
        } else {
            this.ingestMethod = table.getConnector().getDefaultIngestMethod();
        }
        int n;
        try {
            n = Integer.parseInt(this.options.getOrDefault(YdbOptions.BATCH_SIZE, "500"));
        } catch (NumberFormatException nfe) {
            LOG.warn("Illegal input value for option [{}], default of 500 is used instead",
                    YdbOptions.BATCH_SIZE, nfe);
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

    public StructType getTableType() {
        return tableType;
    }

    public StructType getInputType() {
        return inputType;
    }

    public boolean isMapByNames() {
        return mapByNames;
    }

    public int getMaxBulkRows() {
        return maxBulkRows;
    }

}
