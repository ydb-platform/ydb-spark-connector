package tech.ydb.spark.connector;

/**
 * YDB authentication mode selector.
 *
 * @author zinal
 */
public enum YdbAuthMode {

    /**
     * Anonymous access
     */
    NONE,

    /**
     * Set up authentication through environment variables, as specified here:
     * https://ydb.tech/en/docs/reference/ydb-sdk/auth#env
     */
    ENV,

    /**
     * Authentication through a pre-obtained short living connection token
     */

    TOKEN,

    /**
     * Cloud virtual machine or serverless function metadata authentication
     */
    META,

    /**
     * Service account key file authentication
     */
    KEY,

    /**
     * Username and password authentication
     */
    STATIC;

    public static YdbAuthMode fromString(String v) {
        if (v==null) {
            v = "";
        } else {
            v = v.trim();
        }
        if (v.length()==0) {
            return ENV; // default mode is ENV now
        }
        for (YdbAuthMode x : values()) {
            if (v.equalsIgnoreCase(x.name()))
                return x;
        }
        throw new IllegalArgumentException(v);
    }

}
