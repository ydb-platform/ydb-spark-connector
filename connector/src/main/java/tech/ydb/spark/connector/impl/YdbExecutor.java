package tech.ydb.spark.connector.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.DescribePathResult;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.values.ListValue;

/**
 * YDB Database Connector.
 * YDB Connector implements connection initialization and session pool management.
 *
 * @author zinal
 */
public class YdbExecutor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(YdbExecutor.class);

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final QueryClient queryClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;
    private final tech.ydb.query.tools.SessionRetryContext queryRetryCtx;

    public YdbExecutor(GrpcTransport transport, TableClient tableClient, QueryClient queryClient) {
        this.transport = transport;
        this.tableClient = tableClient;
        this.queryClient = queryClient;
        this.schemeClient = SchemeClient.newClient(transport).build();

        this.retryCtx = SessionRetryContext.create(tableClient)
                .sessionCreationTimeout(Duration.ofMinutes(5))
                .idempotent(true)
                .maxRetries(20)
                .build();
        this.queryRetryCtx = tech.ydb.query.tools.SessionRetryContext.create(queryClient)
                .sessionCreationTimeout(Duration.ofMinutes(5))
                .idempotent(true)
                .maxRetries(20)
                .build();
    }

    @Override
    public void close() {
        queryClient.close();
        tableClient.close();
        schemeClient.close();
        transport.close();
    }

    public String extractPath(String name) {
        if (name == null) {
            return transport.getDatabase();
        }
        if (name.startsWith("/")) {
            return name;
        }

        return transport.getDatabase() + "/" + name;
    }

    public SessionRetryContext createRetryCtx(int retryCount, boolean idempotent) {
        return SessionRetryContext.create(tableClient)
                .sessionCreationTimeout(Duration.ofMinutes(5))
                .idempotent(idempotent)
                .maxRetries(retryCount)
                .build();
    }

    public CompletableFuture<Status> executeBulkUpsert(String tablePath, ListValue batch) {
        BulkUpsertSettings settings = new BulkUpsertSettings();
        return retryCtx.supplyStatus(s -> s.executeBulkUpsert(tablePath, batch, settings));
    }

    public CompletableFuture<Status> executeSchemeQuery(String query) {
        return retryCtx.supplyStatus(s -> s.executeSchemeQuery(query));
    }

    public boolean truncateTable(String tablePath) {
        final YdbTruncateTable action = new YdbTruncateTable(tablePath);
        retryCtx.supplyStatus(session -> action.run(session)).join().expectSuccess();
        return true;
    }

    public TableDescription describeTable(String tablePath, boolean includeKeyShards) {
        DescribeTableSettings settings = new DescribeTableSettings();
        settings.setIncludeShardKeyBounds(includeKeyShards);
        Result<TableDescription> result = retryCtx.supplyResult(s -> s.describeTable(tablePath, settings)).join();

        if (result.getStatus().getCode() == StatusCode.SCHEME_ERROR) { // usally not found
            return null;
        }

        result.getStatus().expectSuccess("Cannot describe table " + tablePath);
        return result.getValue();
    }

    public void createTable(String tablePath, TableDescription description) {
        CreateTableSettings settings = new CreateTableSettings();
        retryCtx.supplyStatus(session -> session.createTable(tablePath, description, settings))
                .join()
                .expectSuccess("Cannot create table " + tablePath);
    }

    public void alterTable(String tablePath, AlterTableSettings settings) {
        retryCtx.supplyStatus(session -> session.alterTable(tablePath, settings))
                .join()
                .expectSuccess("Cannot alter table " + tablePath);
    }

    public boolean dropTable(String tablePath) {
        Status status = retryCtx.supplyStatus(session -> session.dropTable(tablePath)).join();
        if (status.isSuccess()) {
            return true;
        }
        logger.warn("Cannot drop table {} - {}", tablePath, status);
        return false;
    }

    public void renameTable(String path1, String path2) {
        retryCtx.supplyStatus(session -> session.renameTable(path1, path2, false))
                .join()
                .expectSuccess("Cannot rename table " + path1);
    }

    public ListDirectoryResult listDirectory(String path) {
        Result<ListDirectoryResult> result = retryCtx
                .supplyResult(session -> schemeClient.listDirectory(path))
                .join();

        if (result.getStatus().getCode() == StatusCode.SCHEME_ERROR) { // usally not found
            return null;
        }

        result.getStatus().expectSuccess("Cannot list directory " + path);
        return result.getValue();
    }

    public boolean makeDirectory(String path) {
        Status status = retryCtx
                .supplyStatus(session -> schemeClient.makeDirectory(path))
                .join();

        if (status.isSuccess() && status.getIssues() != null) {
            for (Issue i : status.getIssues()) {
                String msg = i.getMessage();
                if (msg != null && msg.contains(" path exist, request accepts it")) {
                    return false; // Already exists
                }
            }
        }

        status.expectSuccess("Cannot make directory " + path);
        return true;
    }

    public DescribePathResult describeDirectory(String path) {
        Result<DescribePathResult> result = retryCtx
                .supplyResult(session -> schemeClient.describePath(path))
                .join();

        if (result.getStatus().getCode() == StatusCode.SCHEME_ERROR) { // usally not found
            return null;
        }

        result.getStatus().expectSuccess("Cannot describe directory " + path);
        return result.getValue();
    }

    public boolean removeDirectory(String path) {
        return retryCtx
                .supplyStatus(session -> schemeClient.removeDirectory(path))
                .join()
                .isSuccess();
    }

    public Result<Session> createTableSession() {
        return tableClient.createSession(Duration.ofMinutes(5)).join();
    }

    public Result<QuerySession> createQuerySession() {
        return queryClient.createSession(Duration.ofMinutes(5)).join();
    }

    public List<String> getTabletIds(String path) {
        String query = "SELECT DISTINCT(TabletId) FROM `" + extractPath(path) + "/.sys/primary_index_stats`";
        Result<QueryReader> res = queryRetryCtx.supplyResult(
                session -> QueryReader.readFrom(session.createQuery(query, TxMode.SNAPSHOT_RO))
        ).join();

        if (!res.isSuccess()) {
            return Collections.emptyList();
        }

        ResultSetReader rs = res.getValue().getResultSet(0);
        List<String> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(Long.toUnsignedString(rs.getColumn(0).getUint64()));
        }
        return ids;
    }
}
