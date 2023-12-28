package tech.ydb.spark.connector;

/**
 * YDB data ingestion method.
 *
 * @author mzinal
 */
public enum YdbIngestMethod {
    UPSERT,
    REPLACE,
    BULK
}
