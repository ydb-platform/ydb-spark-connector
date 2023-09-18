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

    /**
     * true to return dates and timestamps as strings, false otherwise. Default false.
     */
    public static final String YDB_DATE_AS_STRING = "date.as.string";

    /**
     * YDB table name to be accessed, in YDB syntax ('/' as path separators).
     */
    public static final String YDB_TABLE = "table";

    /**
     * Connection identity properties used to define the connection singletons.
     */
    public static final List<String> YDB_CONNECTION_IDENTITY =
            Collections.unmodifiableList(Arrays.asList(YDB_URL,
                    YDB_AUTH_MODE, YDB_AUTH_LOGIN, YDB_AUTH_KEY_FILE, YDB_AUTH_TOKEN));

    /**
     * Check whether existing connection's properties matches the provided referenced values.
     * Only important values are checked.
     * 
     * @param existing properties for the existing connection
     * @param referenced properties for the connection to be found or created
     * @return true, if properties connectionMatches, false otherwise
     */
    public static boolean connectionMatches(Map<String, String> existing, Map<String, String> referenced) {
        for (String propName : YDB_CONNECTION_IDENTITY) {
            String v1 = existing.get(propName);
            String v2 = referenced.get(propName);
            if ( ! Objects.equals(v1, v2) )
                return false;
        }
        return true;
    }
}
