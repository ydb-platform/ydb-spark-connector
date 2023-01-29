package tech.ydb.spark.connector;

/**
 *
 * @author zinal
 */
public enum YdbAuthMode {
    
    NONE,
    ENV,
    TOKEN,
    KEY,
    STATIC;

    public static YdbAuthMode fromString(String v) {
        if (v==null)
            v = "";
        else
            v = v.trim();
        if (v.length()==0)
            return ENV;
        for (YdbAuthMode x : values()) {
            if (v.equalsIgnoreCase(x.name()))
                return x;
        }
        throw new IllegalArgumentException(v);
    }

}
