package tech.ydb.spark.connector.common;


/**
 * YDB connection configuration options.
 *
 * @author zinal
 */
public enum OperationOption implements SparkOption {
    /**
     * Number of retries for atomic write operations. Default 10.
     */
    WRITE_RETRY_COUNT("write.retry.count"),

    /**
     * Scan queue depth for each executor. Default 10, minimum 2.
     */
    READQUEUE_SIZE("scan.queue.depth"),

    /**
     * true to list indexes as tables, false otherwise. Default false.
     */
    LIST_INDEXES("list.indexes"),

    /**
     * true to list system directories like `.sys`. Default false.
     */
    LIST_HIDDEN("list.hidden"),

    /**
     * true to return dates and timestamps as strings, false otherwise. Default false.
     */
    DATE_AS_STRING("date.as.string"),

    /**
     * YDB table name to be accessed, in YDB syntax ('/' as path separators).
     */
    DBTABLE("dbtable"),

    /**
     * YQL query text for executing
     */
    DBQUERY("query"),

    /**
     * YDB data ingestion method: upsert/replace/bulk.
     */
    INGEST_METHOD("method"),

    /**
     * Force to use ReadTable for row-oriented tables
     */
    USE_READ_TABLE("useReadTable"),

    /**
     * YDB max batch rows for ingestion.
     */
    BATCH_ROWS("batch.rows"),

    /**
     * Limit for batch request size in bytes
     */
    BATCH_LIMIT("batch.sizelimit"),

    /**
     * Count of parallel batch requests per one writer
     */
    BATCH_CONCURRENCY("batch.concurrency"),

    /**
     * YDB table's primary key, as a comma-delimited list of column names.
     */
    TABLE_AUTOCREATE("table.autocreate"),

    /**
     * YDB table type: ROW - row-organized table, COLUMN - column-organized table, INDEX - secondary index table
     */
    TABLE_TYPE("table.type"),

    /**
     * YDB table path.
     */
    TABLE_PATH("table.path"),

    /**
     * YDB table's automatic primary key column name to be filled by the YDB Spark Connector.
     */
    TABLE_AUTOPK_NAME("table.auto_pk_name"),

    /**
     * YDB table's primary key, as a comma-delimited list of column names.
     */
    TABLE_PRIMARY_KEYS("table.primary_keys"),

    /**
     * YDB table's truncate option when writing to the existing table.
     */
    TABLE_TRUNCATE("table.truncate"),

    TABLE_USE_SIGNED_DATETYPES("table.useSignedDatetypes"),


    PUSHDOWN_PREDICATE("pushDownPredicate"),
    PUSHDOWN_AGGREGATE("pushDownAggregate"),
    PUSHDOWN_LIMIT("pushDownLimit"),
    PUSHDOWN_OFFSET("pushDownOffset");

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
