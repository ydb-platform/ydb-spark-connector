package tech.ydb.spark.connector.common;


/**
 * YDB connection configuration options.
 *
 * @author zinal
 */
public enum OperationOption implements SparkOption {
    /**
     * Use single partition for scanning each table, if true. Default false.
     */
    SCAN_SINGLE("scan.single"),

    /**
     * Scan queue depth for each executor. Default 10, minimum 2.
     */
    SCAN_QUEUE_DEPTH("scan.queue.depth"),

    /**
     * true to list indexes as tables, false otherwise. Default false.
     */
    LIST_INDEXES("list.indexes"),

    /**
     * true to return dates and timestamps as strings, false otherwise. Default false.
     */
    DATE_AS_STRING("date.as.string"),

    /**
     * YDB table name to be accessed, in YDB syntax ('/' as path separators).
     */
    DBTABLE("dbtable"),

    /**
     * YDB data ingestion method: upsert/replace/bulk.
     */
    INGEST_METHOD("method"),

    /**
     * YDB max batch rows for ingestion.
     */
    BATCH_SIZE("batchsize"),

    /**
     * YDB table's primary key, as a comma-delimited list of column names.
     */
    PRIMARY_KEY("primary_key"),

    /**
     * YDB table's automatic primary key column name to be filled by the YDB Spark Connector.
     */
    AUTO_PK("auto_pk"),

    /**
     * YDB table's truncate option when writing to the existing table.
     */
    TRUNCATE("truncate"),

    /**
     * YDB table type: - row-organized (table), - secondary index (index), - column-organized
     * (columnshard).
     */
    TABLE_TYPE("table_type"),

    /**
     * YDB table path.
     */
    TABLE_PATH("table_path");

    public static final String DEFAULT_AUTO_PK = "_spark_key";

    private final String code;

    OperationOption(String code) {
        this.code = code;
    }

    @Override
    public String getCode() {
        return code;
    }
}
