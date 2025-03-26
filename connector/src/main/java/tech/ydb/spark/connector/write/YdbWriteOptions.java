package tech.ydb.spark.connector.write;

import java.io.Serializable;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.common.IngestMethod;
import tech.ydb.spark.connector.common.OperationOption;

/**
 * All settings for YDB upsert operations, shared between partition writers.
 *
 * @author zinal
 */
public class YdbWriteOptions implements Serializable {
    private static final long serialVersionUID = 8780674928826518238L;

    private final YdbTable table;
    private final StructType inputType;
    private final boolean mapByNames;
    private final String additionalPk;
    private final String queryId;
    private final IngestMethod ingestMethod;
    private final int maxBatchSize;
    private final boolean truncate;

    public YdbWriteOptions(YdbTable table, boolean mapByNames, StructType inputType, String queryId,
            CaseInsensitiveStringMap options, boolean truncate) {
        this.table = table;
        this.inputType = inputType;
        this.mapByNames = mapByNames;
        this.additionalPk = detectGeneratedPk(OperationOption.AUTO_PK.read(options), table.schema(), this.inputType);
        this.queryId = queryId;
        this.ingestMethod = OperationOption.INGEST_METHOD.readEnum(options, IngestMethod.BULK_UPSERT);
        this.maxBatchSize = OperationOption.BATCH_SIZE.readInt(options, 500);
        this.truncate = truncate;
    }

    public String getQueryId() {
        return queryId;
    }

    public IngestMethod getIngestMethod() {
        return ingestMethod;
    }

    public YdbTable getTable() {
        return table;
    }

    public StructType getInputType() {
        return inputType;
    }

    public boolean isMapByNames() {
        return mapByNames;
    }

    public String getAddtitionalPk() {
        return additionalPk;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
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
    private static String detectGeneratedPk(String pkName, StructType tableType, StructType inputType) {
        for (String name : tableType.fieldNames()) {
            if (name.equals(pkName)) {
                return null;
            }
        }
        for (String name : inputType.fieldNames()) {
            if (name.equals(pkName)) {
                return null;
            }
        }
        return pkName;
    }

}
