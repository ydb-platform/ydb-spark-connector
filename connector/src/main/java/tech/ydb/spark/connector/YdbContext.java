package tech.ydb.spark.connector;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.AuthRpcProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.EnvironAuthProvider;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.core.impl.auth.GrpcAuthRpc;
import tech.ydb.spark.connector.common.ConnectionOption;
import tech.ydb.spark.connector.impl.YdbExecutor;
import tech.ydb.table.TableClient;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbContext implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(YdbContext.class);

    private static final long serialVersionUID = 6522842483896983993L;

    private final String connectionString;

    private final byte[] caCertBytes;

    private final boolean useMetadata;
    private final boolean useEnv;

    private final String token;
    private final String saKey;

    private final String username;
    private final String password;

    private final int sessionPoolSize;

    public YdbContext(CaseInsensitiveStringMap options) {
        this.connectionString = ConnectionOption.URL.read(options);
        if (connectionString  == null || this.connectionString .trim().isEmpty()) {
            throw new IllegalArgumentException("Incorrect value for property " + ConnectionOption.URL);
        }

        this.caCertBytes = readCaCertificate(options);
        this.useMetadata = ConnectionOption.AUTH_METADATA.readBoolean(options, false);
        this.useEnv = ConnectionOption.AUTH_ENV.readBoolean(options, false);

        this.token = ConnectionOption.AUTH_TOKEN.read(options);
        this.saKey = readSaKey(options);

        this.username = ConnectionOption.AUTH_LOGIN.read(options);
        this.password = ConnectionOption.AUTH_PASSWORD.read(options);

        this.sessionPoolSize = ConnectionOption.POOL_SIZE.readInt(options, 0);
    }

    @Override
    public String toString() {
        return "YDBContext{" + connectionString + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                connectionString, caCertBytes, useMetadata, useEnv, token, saKey, username, password, sessionPoolSize
        );
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (other.getClass() != YdbContext.class) {
            return false;
        }

        YdbContext o = (YdbContext) other;
        return Objects.equals(connectionString, o.connectionString) &&
                Arrays.equals(caCertBytes, o.caCertBytes) &&
                Objects.equals(token, o.token) &&
                Objects.equals(saKey, o.saKey) &&
                Objects.equals(username, o.username) &&
                Objects.equals(password, o.password) &&
                useMetadata == o.useMetadata &&
                useEnv == o.useEnv &&
                sessionPoolSize == sessionPoolSize;
    }

    public YdbExecutor getExecutor() {
        return YdbRegistry.getOrCreate(this, YdbContext::createExecutor);
    }

    private YdbExecutor createExecutor() {
        logger.info("{} is creating executor", this);
        GrpcTransport transport = createGrpcTransport();
        TableClient client = TableClient.newClient(transport)
                .sessionPoolSize(0, getSessionPoolSize())
                .build();
        return new YdbExecutor(transport, client);
    }

    private GrpcTransport createGrpcTransport() {
        GrpcTransportBuilder builder = GrpcTransport.forConnectionString(connectionString.trim())
                .withAuthProvider(createAuthProvider());
        if (caCertBytes != null) {
            builder = builder.withSecureConnection(caCertBytes);
        }

        return builder.build();
    }

    private int getSessionPoolSize() {
        if (sessionPoolSize >= 1) {
            return sessionPoolSize;
        }

        return Math.min(8, 4 * Runtime.getRuntime().availableProcessors());
    }

    private AuthRpcProvider<? super GrpcAuthRpc> createAuthProvider() {
        if (useEnv) {
            return new EnvironAuthProvider();
        }

        if (useMetadata) {
            return CloudAuthHelper.getMetadataAuthProvider();
        }

        if (token != null && !token.trim().isEmpty())  {
            return new TokenAuthProvider(token.trim());
        }

        if (saKey != null && !saKey.trim().isEmpty()) {
            return CloudAuthHelper.getServiceAccountJsonAuthProvider(saKey);
        }

        if (username != null && !username.trim().isEmpty()) {
            return new StaticCredentials(username.trim(), password == null ? "" : password);
        }

        return NopAuthProvider.INSTANCE;
    }

    private static byte[] readCaCertificate(CaseInsensitiveStringMap options) {
        String caFile = ConnectionOption.CA_FILE.read(options);
        if (caFile != null) {
            try {
                return Files.readAllBytes(Paths.get(caFile));
            } catch (IOException ix) {
                throw new IllegalArgumentException("Failed to read CA certificate file " + caFile, ix);
            }
        }

        String caText = ConnectionOption.CA_TEXT.read(options);
        if (caText != null) {
            caText = caText.replace("\\n", "\n");
            return caText.getBytes(StandardCharsets.UTF_8);
        }

        return null;
    }

    private static String readSaKey(CaseInsensitiveStringMap options) {
        String saKeyPath = ConnectionOption.AUTH_SAKEY_FILE.read(options);
        if (saKeyPath != null && !saKeyPath.trim().isEmpty()) {
            try {
                return new String(Files.readAllBytes(Paths.get(saKeyPath)), StandardCharsets.UTF_8);
            } catch (IOException ix) {
                throw new IllegalArgumentException("Failed to read service account key file " + saKeyPath, ix);
            }
        }

        String saKeyJson = ConnectionOption.AUTH_SAKEY_TEXT.read(options);
        if (saKeyJson != null && !saKeyJson.isEmpty()) {
            return saKeyJson;
        }

        return null;
    }
}
