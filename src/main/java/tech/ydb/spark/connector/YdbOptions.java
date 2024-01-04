package tech.ydb.spark.connector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * YDB connection configuration options.
 * 
 * @author zinal
 */
public abstract class YdbOptions {

    /**
     * YDB connection URL. Typically in the following form:
     * grpcs://host:port/?database=/Domain/dbname
     */
    public static final String URL = "url";

    /**
     * Session pool size limit. Default is 4x number of cores available.
     */
    public static final String POOL_SIZE = "pool.size";

    /**
     * CA certificates in the file.
     */
    public static final String CA_FILE = "ca.file";

    /**
     * CA certificates as the literal text.
     */
    public static final String CA_TEXT = "ca.text";

    /**
     * Authentication mode. One of the values defined in @see YdbAuthMode
     */
    public static final String AUTH_MODE = "auth.mode";

    /**
     * Username for the STATIC authentication mode.
     */
    public static final String AUTH_LOGIN = "auth.login";

    /**
     * Password for the STATIC authentication mode.
     */
    public static final String AUTH_PASSWORD = "auth.password";

    /**
     * Service account key file for the KEY authentication mode.
     */
    public static final String AUTH_SAKEY_FILE = "auth.sakey.file";

    /**
     * Service account key as text for the KEY authentication mode.
     */
    public static final String AUTH_SAKEY_TEXT = "auth.sakey.text";

    /**
     * Token value for the TOKEN authentication mode.
     */
    public static final String AUTH_TOKEN = "auth.token";

    /**
     * true to list indexes as tables, false otherwise. Default false.
     */
    public static final String LIST_INDEXES = "list.indexes";

    /**
     * true to return dates and timestamps as strings, false otherwise. Default false.
     */
    public static final String DATE_AS_STRING = "date.as.string";

    /**
     * YDB table name to be accessed, in YDB syntax ('/' as path separators).
     */
    public static final String DBTABLE = "dbtable";

    /**
     * YDB data ingestion method: upsert/replace/bulk.
     */
    public static final String INGEST_METHOD = "method";

    /**
     * YDB max batch rows for ingestion.
     */
    public static final String BATCH_SIZE = "batchsize";

    /**
     * Partitioning setting - AUTO_PARTITIONING_BY_SIZE.
     */
    public static final String AP_BY_SIZE = "AUTO_PARTITIONING_BY_SIZE";

    /**
     * Partitioning setting - AUTO_PARTITIONING_BY_LOAD.
     */
    public static final String AP_BY_LOAD = "AUTO_PARTITIONING_BY_LOAD";

    /**
     * Partitioning setting - AUTO_PARTITIONING_MIN_PARTITIONS_COUNT.
     */
    public static final String AP_MIN_PARTS = "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT";

    /**
     * Partitioning setting - AUTO_PARTITIONING_MAX_PARTITIONS_COUNT.
     */
    public static final String AP_MAX_PARTS = "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT";

    /**
     * Partitioning setting - AUTO_PARTITIONING_PARTITION_SIZE_MB.
     */
    public static final String AP_PART_SIZE_MB = "AUTO_PARTITIONING_PARTITION_SIZE_MB";

    /**
     * Connection identity properties used to define the connection singletons.
     */
    public static final List<String> CONN_IDENTITY =
            Collections.unmodifiableList(Arrays.asList(URL,
                    AUTH_MODE, AUTH_LOGIN, AUTH_SAKEY_FILE, AUTH_TOKEN));

    /**
     * Check whether existing connection's properties matches the provided referenced values.
     * Only important values are checked.
     * 
     * @param existing properties for the existing connection
     * @param referenced properties for the connection to be found or created
     * @return true, if properties connectionMatches, false otherwise
     */
    public static boolean connectionMatches(Map<String, String> existing, Map<String, String> referenced) {
        for (String propName : CONN_IDENTITY) {
            String v1 = existing.get(propName);
            String v2 = referenced.get(propName);
            if ( ! Objects.equals(v1, v2) )
                return false;
        }
        return true;
    }
}
