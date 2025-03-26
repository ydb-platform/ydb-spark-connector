package tech.ydb.spark.connector.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.AuthRpcProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.EnvironAuthProvider;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.core.impl.auth.GrpcAuthRpc;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.spark.connector.common.ConnectionOption;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 * YDB Database Connector.
 * YDB Connector implements connection initialization and session pool management.
 *
 * @author zinal
 */
public class YdbConnector implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(YdbConnector.class);

    private final String catalogName;
    private final CaseInsensitiveStringMap options;

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;

    public YdbConnector(String catalogName, CaseInsensitiveStringMap options) {
        this.catalogName = catalogName;
        this.options = options;

        String connectionString = ConnectionOption.URL.read(options);
        if (connectionString == null || connectionString.trim().isEmpty()) {
            throw new IllegalArgumentException("Incorrect value for property " + ConnectionOption.URL);
        }

        GrpcTransportBuilder builder = GrpcTransport.forConnectionString(connectionString.trim());

        byte[] certificate = readCaCertificate(options);
        if (certificate != null) {
            builder = builder.withSecureConnection(certificate);
        }

        AuthRpcProvider<? super GrpcAuthRpc> provider = readAuthProvider(options);
        if (provider != null) {
            builder = builder.withAuthProvider(provider);
        }

        this.transport = builder.build();

        int defaultPoolSize = 4 * Runtime.getRuntime().availableProcessors();
        int poolSize = ConnectionOption.POOL_SIZE.readInt(options, defaultPoolSize);
        if (poolSize < 1) {
            logger.warn("Incorrect value {} for session pool size, use default {}", poolSize, defaultPoolSize);
            poolSize = defaultPoolSize;
        }
        this.tableClient = TableClient.newClient(transport)
                .sessionPoolSize(1, poolSize)
                .build();
        this.schemeClient = SchemeClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient)
                .idempotent(true)
                .maxRetries(20)
                .build();
    }

    @Override
    public void close() {
        tableClient.close();
        schemeClient.close();
        transport.close();
    }

    @Override
    public String toString() {
        return "YdbConnector{" + "catalogName=" + catalogName + ", database=" + transport.getDatabase() + '}';
    }

    public CaseInsensitiveStringMap getOptions() {
        return options;
    }

    public String getCatalogName() {
        return catalogName;
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

    private static byte[] readCaCertificate(CaseInsensitiveStringMap options) {
        String caFile = ConnectionOption.CA_FILE.read(options);
        if (caFile != null) {
            try {
                return Files.readAllBytes(Paths.get(caFile));
            } catch (IOException ix) {
                throw new RuntimeException("Failed to read CA file " + caFile, ix);
            }
        }

        String caText = ConnectionOption.CA_TEXT.read(options);
        if (caText != null) {
            caText = caText.replace("\\n", "\n");
            return caText.getBytes(StandardCharsets.UTF_8);
        }

        return null;
    }

    private static AuthRpcProvider<? super GrpcAuthRpc> readAuthProvider(CaseInsensitiveStringMap options) {
        if (ConnectionOption.AUTH_ENV.readBoolean(options, false)) {
            return new EnvironAuthProvider();
        }

        if (ConnectionOption.AUTH_METADATA.readBoolean(options, false)) {
            return CloudAuthHelper.getMetadataAuthProvider();
        }

        String token = ConnectionOption.AUTH_TOKEN.read(options);
        if (token != null && !token.trim().isEmpty()) {
            return new TokenAuthProvider(token.trim());
        }

        String saKeyPath = ConnectionOption.AUTH_SAKEY_FILE.read(options);
        if (saKeyPath != null && !saKeyPath.trim().isEmpty()) {
            return CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyPath);
        }

        String saKeyJson = ConnectionOption.AUTH_SAKEY_TEXT.read(options);
        if (saKeyJson != null && !saKeyJson.isEmpty()) {
            return CloudAuthHelper.getServiceAccountJsonAuthProvider(saKeyJson);
        }

        String username = ConnectionOption.AUTH_LOGIN.read(options);
        if (username != null && !username.trim().isEmpty()) {
            String password = ConnectionOption.AUTH_PASSWORD.read(options);
            if (password == null) {
                password = "";
            }
            return new StaticCredentials(username.trim(), password);
        }

        return null;
    }
}
