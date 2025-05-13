package tech.ydb.spark.connector.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.DescribePathResult;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.Params;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;

/**
 * YDB Database Connector.
 * YDB Connector implements connection initialization and session pool management.
 *
 * @author zinal
 */
public class YdbExecutor implements AutoCloseable {
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final QueryClient queryClient;
    private final SchemeClient schemeClient;
    private final ImplicitSession implicitSession;
    private final SessionRetryContext retryCtx;
    private final SessionRetryContext implicitRetryCtx;

    public YdbExecutor(GrpcTransport transport, TableClient tableClient, QueryClient queryClient) {
        this.transport = transport;
        this.tableClient = tableClient;
        this.queryClient = queryClient;
        this.schemeClient = SchemeClient.newClient(transport).build();
        this.implicitSession = new ImplicitSession(transport);

        this.retryCtx = SessionRetryContext.create(tableClient)
                .sessionCreationTimeout(Duration.ofMinutes(5))
                .idempotent(true)
                .maxRetries(20)
                .build();
        this.implicitRetryCtx = SessionRetryContext.create(implicitSession)
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

    public GrpcReadStream<ReadTablePart> executeReadTable(String tablePath, ReadTableSettings settings) {
        return implicitSession.executeReadTable(tablePath, settings);
    }

    public CompletableFuture<Status> executeBulkUpsert(String tablePath, ListValue batch) {
        BulkUpsertSettings settings = new BulkUpsertSettings();
        return implicitRetryCtx.supplyStatus(s -> s.executeBulkUpsert(tablePath, batch, settings));
    }

    public CompletableFuture<Status> executeDataQuery(String query, Params params) {
        return retryCtx.supplyStatus(
                s -> s.executeDataQuery(query, TxControl.serializableRw(), params).thenApply(Result::getStatus)
        );
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
        return retryCtx.supplyStatus(session -> session.dropTable(tablePath))
                .join()
                .isSuccess();
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

    public Result<QuerySession> createQuerySession() {
        return queryClient.createSession(Duration.ofMinutes(5)).join();
    }
}
