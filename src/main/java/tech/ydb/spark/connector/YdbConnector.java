package tech.ydb.spark.connector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.auth.iam.CloudAuthIdentity;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.TableClient;

/**
 * YDB Database Connector.
 * YDB Connector implements connection initialization and session pool management.
 *
 * @author zinal
 */
final class YdbConnector extends YdbOptions implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbConnector.class);

    private final String catalogName;
    private final Map<String,String> connectOptions;
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;
    private final String database;
    private final YdbTypes defaultTypes;

    public YdbConnector(String catalogName, Map<String, String> props) {
        this.catalogName = catalogName;
        this.connectOptions = new HashMap<>();
        for (Map.Entry<String,String> me : props.entrySet()) {
            this.connectOptions.put(me.getKey().toLowerCase(), me.getValue());
        }
        this.defaultTypes = new YdbTypes(this.connectOptions);
        final int poolSize;
        try {
            int ncores = Runtime.getRuntime().availableProcessors();
            if (ncores < 2)
                ncores = 2;
            String defaultCores = String.valueOf(4 * ncores);
            poolSize = Integer.parseInt(props.getOrDefault(YDB_POOL_SIZE, defaultCores));
        } catch(NumberFormatException nfe) {
            throw new IllegalArgumentException("Incorrect value for property " + YDB_POOL_SIZE, nfe);
        }
        GrpcTransportBuilder builder = GrpcTransport
                .forConnectionString(props.get(YDB_URL));
        String caString = props.get(YDB_CA_FILE);
        if (caString!=null) {
            byte[] cert;
            try {
                cert = Files.readAllBytes(Paths.get(caString));
            } catch(IOException ix) {
                throw new RuntimeException("Failed to read CA file " + caString, ix);
            }
            builder = builder.withSecureConnection(cert);
        } else {
            caString = props.get(YDB_CA_TEXT);
            caString = caString.replace("\\n", "\n");
            builder = builder.withSecureConnection(caString.getBytes(StandardCharsets.UTF_8));
        }
        final YdbAuthMode authMode;
        try {
            authMode = YdbAuthMode.fromString(props.get(YDB_AUTH_MODE));
        } catch(IllegalArgumentException iae) {
            throw new IllegalArgumentException("Incorrect value for property " + YDB_AUTH_MODE, iae);
        }
        switch (authMode) {
            case ENV:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getAuthProviderFromEnviron());
                break;
            case META:
                builder = builder.withAuthProvider(opt -> CloudAuthIdentity.metadataIdentity());
                break;
            case STATIC:
                builder = builder.withAuthProvider(
                    new StaticCredentials(props.get(YDB_AUTH_LOGIN), props.get(YDB_AUTH_PASSWORD)));
                break;
            case KEY:
                String keyFile = props.get(YDB_AUTH_SAKEY_FILE);
                if (keyFile!=null) {
                    final String v = keyFile;
                    builder = builder.withAuthProvider((opt) -> {
                        return CloudAuthIdentity.serviceAccountIdentity(Paths.get(v));
                    });
                } else {
                    keyFile = props.get(YDB_AUTH_SAKEY_TEXT);
                    final String v = keyFile;
                    builder = builder.withAuthProvider((opt) -> {
                        return CloudAuthIdentity.serviceAccountIdentity(v);
                    });
                }
                break;
            case TOKEN:
                final String authToken = props.get(YDB_AUTH_TOKEN);
                builder = builder.withAuthProvider((opt) -> {
                    return CloudAuthIdentity.iamTokenIdentity(authToken);
                });
                break;
            case NONE:
                break;
        }
        GrpcTransport gt = builder.build();
        this.database = gt.getDatabase();
        try {
            this.tableClient = TableClient.newClient(gt)
                    .sessionPoolSize(0, poolSize)
                    .build();
            this.schemeClient = SchemeClient.newClient(gt).build();
            this.retryCtx = SessionRetryContext.create(tableClient).build();
            this.transport = gt;
            gt = null; // to avoid closing below
        } finally {
            if (gt != null)
                gt.close();
        }
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Map<String, String> getConnectOptions() {
        return connectOptions;
    }

    public GrpcTransport getTransport() {
        return transport;
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public SchemeClient getSchemeClient() {
        return schemeClient;
    }

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

    public String getDatabase() {
        return database;
    }

    public YdbTypes getDefaultTypes() {
        return defaultTypes;
    }

    @Override
    public void close() {
        if (tableClient != null) {
            try {
                tableClient.close();
            } catch(Exception ex) {
                LOG.warn("TableClient closing threw an exception", ex);
            }
        }
        if (schemeClient != null) {
            try {
                schemeClient.close();
            } catch(Exception ex) {
                LOG.warn("SchemeClient closing threw an exception", ex);
            }
        }
        if (transport != null) {
            try {
                transport.close();
            } catch(Exception ex) {
                LOG.warn("GrpcTransport closing threw an exception", ex);
            }
        }
    }

    @Override
    public String toString() {
        return "YdbConnector{" + "catalogName=" + catalogName + ", database=" + database + '}';
    }

}
