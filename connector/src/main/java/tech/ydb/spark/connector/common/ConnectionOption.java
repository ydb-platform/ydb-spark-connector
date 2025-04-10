package tech.ydb.spark.connector.common;

/**
 *
 * @author Aleksandr Gorshenin
 */
public enum ConnectionOption implements SparkOption {
    /**
     * YDB connection URL. Typically in the following form:
     * grpcs://host:port/Domain/dbname
     */
    URL("url"),

    /**
     * CA certificates in the file.
     */
    CA_FILE("ca.file"),

    /**
     * CA certificates as the literal text.
     */
    CA_TEXT("ca.text"),

    /**
     * Metadata authentication mode.
     */
    AUTH_METADATA("auth.use_metadata"),

    /**
     * Environment authentication mode.
     */
    AUTH_ENV("auth.use_env"),

    /**
     * Username for the static authentication mode.
     */
    AUTH_LOGIN("auth.login"),

    /**
     * Password for the static authentication mode.
     */
    AUTH_PASSWORD("auth.password"),

    /**
     * Service account key file for authentication.
     */
    AUTH_SAKEY_FILE("auth.sakey.file"),

    /**
     * Service account key as text for authentication.
     */
    AUTH_SAKEY_TEXT("auth.sakey.text"),

    /**
     * Token value for the TOKEN authentication mode.
     */
    AUTH_TOKEN("auth.token"),

    /**
     * Session pool size limit. Default is 4x number of cores available.
     */
    POOL_SIZE("pool.size");

    private final String code;

    ConnectionOption(String code) {
        this.code = code;
    }

    @Override
    public String getCode() {
        return code;
    }
}
