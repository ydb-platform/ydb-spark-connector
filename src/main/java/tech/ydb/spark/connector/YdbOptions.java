package tech.ydb.spark.connector;

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
    public static final String YDB_URL = "url";

    /**
     * Session pool size limit. Default is 4x number of cores available.
     */
    public static final String YDB_POOL_SIZE = "pool.size";

    /**
     * Authentication mode. One of the values defined in @see YdbAuthMode
     */
    public static final String YDB_AUTH_MODE = "auth.mode";

    /**
     * Username for the STATIC authentication mode.
     */
    public static final String YDB_AUTH_LOGIN = "auth.login";

    /**
     * Password for the STATIC authentication mode.
     */
    public static final String YDB_AUTH_PASSWORD = "auth.password";

    /**
     * Service account key file for the KEY authentication mode.
     */
    public static final String YDB_AUTH_KEY_FILE = "auth.keyfile";

    /**
     * Token value for the TOKEN authentication mode.
     */
    public static final String YDB_AUTH_TOKEN = "auth.token";

    /**
     * true to list indexes as tables, false otherwise. Default false.
     */
    public static final String YDB_LIST_INDEXES = "list.indexes";
}
