package tech.ydb.spark.connector.write;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.YdbOptions;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.common.YdbIngestMethod;
import tech.ydb.spark.connector.common.YdbTableOperationOptions;

/**
 * All settings for YDB upsert operations, shared between partition writers.
 *
 * @author zinal
 */
public class YdbWriteOptions extends YdbTableOperationOptions implements Serializable {
    private static final long serialVersionUID = 8780674928826518238L;

    private static final Logger LOG = LoggerFactory.getLogger(YdbWriteOptions.class);

    private final StructType tableType;
    private final StructType inputType;
    private final boolean mapByNames;
    private final String generatedPk;
    private final String queryId;
    private final YdbIngestMethod ingestMethod;
    private final HashMap<String, String> options;
    private final int maxBulkRows;
    private final boolean truncate;

    public YdbWriteOptions(YdbTable table, boolean mapByNames, StructType inputType,
            String queryId, Map<String, String> options, boolean truncate) {
        super(table);
        this.tableType = table.schema();
        this.inputType = inputType;
        this.mapByNames = mapByNames;
        this.generatedPk = detectGeneratedPk(this.tableType, this.inputType);
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
        this.truncate = truncate;
    }

    public YdbWriteOptions(YdbWriteOptions src, boolean truncate) {
        super(src);
        this.tableType = src.tableType;
        this.inputType = src.inputType;
        this.mapByNames = src.mapByNames;
        this.generatedPk = src.generatedPk;
        this.queryId = src.queryId;
        this.ingestMethod = src.ingestMethod;
        this.options = new HashMap<>(src.options);
        this.maxBulkRows = src.maxBulkRows;
        this.truncate = truncate;
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

    public String getGeneratedPk() {
        return generatedPk;
    }

    public int getMaxBulkRows() {
        return maxBulkRows;
    }

    public boolean isTruncate() {
        return truncate;
    }

    /**
     * PK generation detection.
     * @param tableType Target table structure
     * @param inputType Input dataset structure
     * @return generated PK column name, if target table has automatic PK column,
     *      and it is not included in the input dataset
     */
    private static String detectGeneratedPk(StructType tableType, StructType inputType) {
        String pkName = null;
        for (String name : tableType.fieldNames()) {
            if (YdbOptions.AUTO_PK.equalsIgnoreCase(name)) {
                pkName = name;
            }
        }
        if (pkName == null) {
            return null;
        }
        for (String name : inputType.fieldNames()) {
            if (pkName.equals(name)) {
                return null;
            }
        }
        return pkName;
    }

}
