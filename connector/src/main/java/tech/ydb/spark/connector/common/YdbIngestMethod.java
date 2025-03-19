package tech.ydb.spark.connector.common;

/**
 * YDB data ingestion method.
 *
 * @author mzinal
 */
public enum YdbIngestMethod {
    UPSERT,
    REPLACE,
    BULK;

    public static YdbIngestMethod fromString(String v) {
        if (v == null) {
            v = "";
        } else {
            v = v.trim();
        }
        if (v.length() == 0) {
            return UPSERT; // default method is UPSERT now
        }
        for (YdbIngestMethod x : values()) {
            if (v.equalsIgnoreCase(x.name())) {
                return x;
            }
        }
        throw new IllegalArgumentException(v);
    }
}
