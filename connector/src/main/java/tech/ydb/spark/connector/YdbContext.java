package tech.ydb.spark.connector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.io.ByteStreams;
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
import tech.ydb.core.utils.URITools;
import tech.ydb.spark.connector.common.ConnectionOption;
import tech.ydb.spark.connector.impl.YdbExecutor;
import tech.ydb.table.TableClient;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbContext implements Serializable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(YdbContext.class);

    private static final long serialVersionUID = 6522842483896983993L;

    // copy URL paramter names from JDBC
    private static final String JDBC_TOKEN_FILE = "tokenFile";
    private static final String JDBC_SECURE_CONNECTION_CERTIFICATE = "secureConnectionCertificate";
    private static final String JDBC_SA_KEY_FILE = "saKeyFile";
    private static final String JDBC_USE_METADATA = "useMetadata";
    private static final String HOME_REF = "~/";

    private final String connectionString;

    private final byte[] caCertBytes;

    private final boolean useMetadata;
    private final boolean useEnv;

    private final String token;
    private final String saKey;

    private final String username;
    private final String password;

    private final int sessionPoolSize;

    public YdbContext(Map<String, String> options) {
        this.connectionString = ConnectionOption.URL.read(options);
        if (connectionString  == null || this.connectionString .trim().isEmpty()) {
            throw new IllegalArgumentException("Incorrect value for property " + ConnectionOption.URL);
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.putAll(parseJdbcParams(connectionString));
        parameters.putAll(options);

        this.caCertBytes = readCaCertificate(parameters);
        this.useMetadata = ConnectionOption.AUTH_METADATA.readBoolean(parameters, false);
        this.useEnv = ConnectionOption.AUTH_ENV.readBoolean(parameters, false);

        this.token = readToken(parameters);
        this.saKey = readSaKey(parameters);

        this.username = ConnectionOption.AUTH_LOGIN.read(parameters);
        this.password = ConnectionOption.AUTH_PASSWORD.read(parameters);

        this.sessionPoolSize = ConnectionOption.POOL_SIZE.readInt(parameters, 0);
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

    @Override
    public void close() {
        YdbRegistry.closeExecutor(this);
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

    private static byte[] readCaCertificate(Map<String, String> options) {
        String caFile = ConnectionOption.CA_FILE.read(options);
        if (caFile != null) {
            try {
                return readFileAsBytes(caFile);
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

    private static String readSaKey(Map<String, String> options) {
        String saKeyPath = ConnectionOption.AUTH_SAKEY_FILE.read(options);
        if (saKeyPath != null && !saKeyPath.trim().isEmpty()) {
            try {
                byte[] content = readFileAsBytes(saKeyPath);
                return new String(content, StandardCharsets.UTF_8);
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

    private static String readToken(Map<String, String> options) {
        String tokenFile = ConnectionOption.AUTH_TOKEN_FILE.read(options);
        if (tokenFile != null && !tokenFile.trim().isEmpty()) {
            try {
                byte[] content = readFileAsBytes(tokenFile);
                return firstLine(content);
            } catch (IOException ix) {
                throw new IllegalArgumentException("Failed to read token file " + tokenFile, ix);
            }
        }

        String tokenValue = ConnectionOption.AUTH_TOKEN.read(options);
        if (tokenValue != null && !tokenValue.trim().isEmpty()) {
            return tokenValue.trim();
        }

        return null;
    }

    private static Map<String, String> parseJdbcParams(String url) {
        Map<String, String> params = new HashMap<>();
        try {
            URI uri = new URI(url.contains("://") ? url : "grpc://" + url);
            URITools.splitQuery(uri).forEach((key, values) -> {
                if (key.equalsIgnoreCase(JDBC_TOKEN_FILE)) {
                    params.put(ConnectionOption.AUTH_TOKEN_FILE.getCode(), values.get(0));
                }
                if (key.equalsIgnoreCase(JDBC_SECURE_CONNECTION_CERTIFICATE)) {
                    params.put(ConnectionOption.CA_FILE.getCode(), values.get(0));
                }
                if (key.equalsIgnoreCase(JDBC_SA_KEY_FILE)) {
                    params.put(ConnectionOption.AUTH_SAKEY_FILE.getCode(), values.get(0));
                }
                if (key.equalsIgnoreCase(JDBC_USE_METADATA)) {
                    params.put(ConnectionOption.AUTH_METADATA.getCode(), values.get(0));
                }
            });
        } catch (URISyntaxException ex) {
            // nothing
        }
        return params;
    }

    public static byte[] readFileAsBytes(String filePath) throws IOException {
        String path = filePath.trim();

        if (path.startsWith(HOME_REF)) {
            String home = System.getProperty("user.home");
            path = home + path.substring(HOME_REF.length() - 1);
        }

        try (InputStream is = new FileInputStream(path)) {
            return ByteStreams.toByteArray(is);
        }
    }

    private static String firstLine(byte[] bytes) throws IOException {
        for (int idx = 0; idx < bytes.length; idx++) {
            if (bytes[idx] == '\n' || bytes[idx] == '\r') {
                return new String(bytes, 0, idx, StandardCharsets.UTF_8);
            }
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
