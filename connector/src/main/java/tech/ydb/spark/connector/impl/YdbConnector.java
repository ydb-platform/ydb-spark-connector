package tech.ydb.spark.connector.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.auth.iam.CloudAuthIdentity;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.spark.connector.YdbAuthMode;
import tech.ydb.spark.connector.YdbIngestMethod;
import tech.ydb.spark.connector.YdbOptions;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 * YDB Database Connector.
 * YDB Connector implements connection initialization and session pool management.
 *
 * @author zinal
 */
public class YdbConnector extends YdbOptions implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbConnector.class);

    private final String catalogName;
    private final Map<String, String> connectOptions;
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;
    private final String database;
    private final YdbTypes defaultTypes;
    private final YdbIngestMethod defaultIngestMethod;
    private final boolean singlePartitionScans;

    public YdbConnector(String catalogName, Map<String, String> props) {
        this.catalogName = catalogName;
        this.connectOptions = new HashMap<>();
        for (Map.Entry<String, String> me : props.entrySet()) {
            this.connectOptions.put(me.getKey().toLowerCase(), me.getValue());
        }
        this.defaultTypes = new YdbTypes(this.connectOptions);
        this.defaultIngestMethod = YdbIngestMethod.fromString(
                this.connectOptions.get(YdbOptions.INGEST_METHOD));
        this.singlePartitionScans = Boolean.parseBoolean(props.getOrDefault(SCAN_SINGLE, "false"));
        int poolSize = getPoolSize(props);
        GrpcTransportBuilder builder = GrpcTransport.forConnectionString(props.get(URL));
        builder = applyCaSettings(builder, props);
        builder = applyAuthSettings(builder, props);
        GrpcTransport gt = builder.build();
        try {
            this.tableClient = TableClient.newClient(gt)
                    .sessionPoolSize(1, poolSize)
                    .build();
            this.schemeClient = SchemeClient.newClient(gt).build();
            this.retryCtx = SessionRetryContext.create(tableClient)
                    .idempotent(true)
                    .maxRetries(20)
                    .build();
            this.transport = gt;
            gt = null; // to avoid closing below
        } finally {
            if (gt != null) {
                gt.close();
            }
        }
        this.database = this.transport.getDatabase();
    }

    private static GrpcTransportBuilder applyCaSettings(GrpcTransportBuilder builder,
            Map<String, String> props) {
        String caString = props.get(CA_FILE);
        if (caString != null) {
            byte[] cert;
            try {
                cert = Files.readAllBytes(Paths.get(caString));
            } catch (IOException ix) {
                throw new RuntimeException("Failed to read CA file " + caString, ix);
            }
            builder = builder.withSecureConnection(cert);
        } else {
            caString = props.get(CA_TEXT);
            if (caString != null) {
                caString = caString.replace("\\n", "\n");
                byte[] cert = caString.getBytes(StandardCharsets.UTF_8);
                builder = builder.withSecureConnection(cert);
            }
        }
        return builder;
    }

    private static GrpcTransportBuilder applyAuthSettings(GrpcTransportBuilder builder,
            Map<String, String> props) {
        final YdbAuthMode authMode;
        try {
            authMode = YdbAuthMode.fromString(props.get(AUTH_MODE));
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Incorrect value for property " + AUTH_MODE, iae);
        }
        switch (authMode) {
            case ENV:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getAuthProviderFromEnviron());
                break;
            case META:
                builder = builder.withAuthProvider((opt) -> CloudAuthIdentity.metadataIdentity(null));
                break;
            case STATIC:
                builder = builder.withAuthProvider(
                        new StaticCredentials(props.get(AUTH_LOGIN), props.get(AUTH_PASSWORD)));
                break;
            case KEY:
                String keyFile = props.get(AUTH_SAKEY_FILE);
                if (keyFile != null) {
                    final String v = keyFile;
                    builder = builder.withAuthProvider((opt) -> {
                        return CloudAuthIdentity.serviceAccountIdentity(Paths.get(v), null);
                    });
                } else {
                    keyFile = props.get(AUTH_SAKEY_TEXT);
                    if (keyFile != null) {
                        final String v = keyFile;
                        builder = builder.withAuthProvider((opt) -> {
                            return CloudAuthIdentity.serviceAccountIdentity(v, null);
                        });
                    }
                }
                break;
            case TOKEN:
                final String authToken = props.get(AUTH_TOKEN);
                builder = builder.withAuthProvider(new TokenAuthProvider(authToken));
                break;
            case NONE:
                break;
            default: // unreached
                throw new UnsupportedOperationException();
        }
        return builder;
    }

    private static int getPoolSize(Map<String, String> props) {
        try {
            int ncores = Runtime.getRuntime().availableProcessors();
            if (ncores < 2) {
                ncores = 2;
            }
            String defaultCores = String.valueOf(4 * ncores);
            return Integer.parseInt(props.getOrDefault(POOL_SIZE, defaultCores));
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Incorrect value for property " + POOL_SIZE, nfe);
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

    public YdbIngestMethod getDefaultIngestMethod() {
        return defaultIngestMethod;
    }

    public boolean isSinglePartitionScans() {
        return singlePartitionScans;
    }

    public int getScanQueueDepth() {
        int scanQueueDepth;
        try {
            scanQueueDepth = Integer.parseInt(connectOptions.getOrDefault(SCAN_QUEUE_DEPTH, "3"));
        } catch (NumberFormatException nfe) {
            LOG.warn("Illegal value of {} property, reverting to default of 3.", SCAN_QUEUE_DEPTH, nfe);
            scanQueueDepth = 3;
        }
        if (scanQueueDepth < 2) {
            LOG.warn("Value of {} property too low, reverting to minimum of 2.", SCAN_QUEUE_DEPTH);
            scanQueueDepth = 2;
        }
        return scanQueueDepth;
    }

    public int getScanSessionSeconds() {
        int scanSessionSeconds;
        try {
            scanSessionSeconds = Integer.parseInt(connectOptions.getOrDefault(SCAN_SESSION_SECONDS, "30"));
        } catch (NumberFormatException nfe) {
            LOG.warn("Illegal value of {} property, reverting to default of 30.", SCAN_SESSION_SECONDS, nfe);
            scanSessionSeconds = 30;
        }
        if (scanSessionSeconds < 1) {
            LOG.warn("Value of {} property too low, reverting to minimum of 1.", SCAN_SESSION_SECONDS);
            scanSessionSeconds = 1;
        }
        return scanSessionSeconds;
    }

    @Override
    public void close() {
        if (tableClient != null) {
            try {
                tableClient.close();
            } catch (Exception ex) {
                LOG.warn("TableClient closing threw an exception", ex);
            }
        }
        if (schemeClient != null) {
            try {
                schemeClient.close();
            } catch (Exception ex) {
                LOG.warn("SchemeClient closing threw an exception", ex);
            }
        }
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception ex) {
                LOG.warn("GrpcTransport closing threw an exception", ex);
            }
        }
    }

    @Override
    public String toString() {
        return "YdbConnector{" + "catalogName=" + catalogName + ", database=" + database + '}';
    }

}
