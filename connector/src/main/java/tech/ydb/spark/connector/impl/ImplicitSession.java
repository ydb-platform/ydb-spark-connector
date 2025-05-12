package tech.ydb.spark.connector.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.core.grpc.GrpcRequestSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.impl.call.ProxyReadStream;
import tech.ydb.core.operation.Operation;
import tech.ydb.proto.StatusCodesProtos;
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.SessionSupplier;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.query.ReadRowsResult;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.TableRpc;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.BeginTxSettings;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.CopyTableSettings;
import tech.ydb.table.settings.CopyTablesSettings;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.DeleteSessionSettings;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.DropTableSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;
import tech.ydb.table.settings.ExplainDataQuerySettings;
import tech.ydb.table.settings.KeepAliveSessionSettings;
import tech.ydb.table.settings.PrepareDataQuerySettings;
import tech.ydb.table.settings.ReadRowsSettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.settings.RenameTablesSettings;
import tech.ydb.table.settings.RollbackTxSettings;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.proto.ProtoValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ImplicitSession implements Session, SessionSupplier {
    private final TableRpc rpc;

    public ImplicitSession(GrpcTransport tranport) {
        this.rpc = GrpcTableRpc.useTransport(tranport);
    }

    @Override
    public CompletableFuture<Result<Session>> createSession(Duration duration) {
        return CompletableFuture.completedFuture(Result.success(this));
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return rpc.getScheduler();
    }

    @Override
    public void close() {
        // nothing
    }

    private GrpcRequestSettings makeGrpcRequestSettings(Duration timeout) {
        return GrpcRequestSettings.newBuilder()
                .withDeadline(timeout)
                .build();
    }

    @Override
    public String getId() {
        return "ImplicitSession";
    }

    @Override
    public CompletableFuture<Status> executeBulkUpsert(String tablePath, ListValue rows, BulkUpsertSettings settings) {
        ValueProtos.TypedValue typedRows = ValueProtos.TypedValue.newBuilder()
                .setType(rows.getType().toPb())
                .setValue(rows.toPb())
                .build();

        YdbTable.BulkUpsertRequest request = YdbTable.BulkUpsertRequest.newBuilder()
                .setTable(tablePath)
                .setRows(typedRows)
                .setOperationParams(Operation.buildParams(settings.toOperationSettings()))
                .build();

        return rpc.bulkUpsert(request, makeGrpcRequestSettings(settings.getTimeoutDuration()));
    }

    @Override
    public CompletableFuture<Result<ReadRowsResult>> readRows(String pathToTable, ReadRowsSettings settings) {
        YdbTable.ReadRowsRequest.Builder requestBuilder = YdbTable.ReadRowsRequest.newBuilder()
                .setPath(pathToTable)
                .addAllColumns(settings.getColumns())
                .setKeys(settings.getKeys().isEmpty() ? ValueProtos.TypedValue.newBuilder().build() :
                    ValueProtos.TypedValue.newBuilder()
                            .setType(ListType.of(settings.getKeys().get(0).getType()).toPb())
                            .setValue(ValueProtos.Value.newBuilder()
                                    .addAllItems(settings.getKeys().stream().map(StructValue::toPb)
                                            .collect(Collectors.toList())))
                            .build());
        return rpc.readRows(requestBuilder.build(), makeGrpcRequestSettings(settings.getRequestTimeout()))
                .thenApply(result -> result.map(ReadRowsResult::new));
    }

    @Override
    public GrpcReadStream<ReadTablePart> executeReadTable(String tablePath, ReadTableSettings settings) {
        YdbTable.ReadTableRequest.Builder request = YdbTable.ReadTableRequest.newBuilder()
                .setPath(tablePath)
                .setOrdered(settings.isOrdered())
                .setRowLimit(settings.getRowLimit())
                .setBatchLimitBytes(settings.batchLimitBytes())
                .setBatchLimitRows(settings.batchLimitRows());

        Value<?> fromKey = settings.getFromKey();
        if (fromKey != null) {
            YdbTable.KeyRange.Builder range = request.getKeyRangeBuilder();
            if (settings.isFromInclusive()) {
                range.setGreaterOrEqual(ProtoValue.toTypedValue(fromKey));
            } else {
                range.setGreater(ProtoValue.toTypedValue(fromKey));
            }
        }

        Value<?> toKey = settings.getToKey();
        if (toKey != null) {
            YdbTable.KeyRange.Builder range = request.getKeyRangeBuilder();
            if (settings.isToInclusive()) {
                range.setLessOrEqual(ProtoValue.toTypedValue(toKey));
            } else {
                range.setLess(ProtoValue.toTypedValue(toKey));
            }
        }

        if (!settings.getColumns().isEmpty()) {
            request.addAllColumns(settings.getColumns());
        }

        final GrpcReadStream<YdbTable.ReadTableResponse> origin = rpc.streamReadTable(
                request.build(), makeGrpcRequestSettings(settings.getRequestTimeout())
        );

        return new ProxyReadStream<>(origin, (response, future, observer) -> {
            StatusCodesProtos.StatusIds.StatusCode statusCode = response.getStatus();
            if (statusCode == StatusCodesProtos.StatusIds.StatusCode.SUCCESS) {
                try {
                    observer.onNext(new ReadTablePart(response.getResult(), response.getSnapshot()));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    origin.cancel();
                }
            } else {
                Issue[] issues = Issue.fromPb(response.getIssuesList());
                StatusCode code = StatusCode.fromProto(statusCode);
                future.complete(Status.of(code, issues));
                origin.cancel();
            }
        });
    }

    @Override
    public CompletableFuture<Status> createTable(String path, TableDescription desc, CreateTableSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Status> dropTable(String path, DropTableSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Status> alterTable(String path, AlterTableSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Status> copyTable(String src, String dst, CopyTableSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Status> copyTables(CopyTablesSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Status> renameTables(RenameTablesSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Result<TableDescription>> describeTable(String path, DescribeTableSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Result<DataQueryResult>> executeDataQuery(
            String query, TxControl<?> txControl, Params params, ExecuteDataQuerySettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Result<DataQuery>> prepareDataQuery(String query, PrepareDataQuerySettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Status> executeSchemeQuery(String query, ExecuteSchemeQuerySettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Result<ExplainDataQueryResult>> explainDataQuery(
            String query, ExplainDataQuerySettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Result<Transaction>> beginTransaction(Transaction.Mode transactionMode,
                                                                   BeginTxSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public TableTransaction createNewTransaction(TxMode txMode) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Result<TableTransaction>> beginTransaction(TxMode txMode, BeginTxSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public GrpcReadStream<ResultSetReader> executeScanQuery(
            String query, Params params, ExecuteScanQuerySettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Status> commitTransaction(String txId, CommitTxSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    @Deprecated
    public CompletableFuture<Status> rollbackTransaction(String txId, RollbackTxSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }

    @Override
    public CompletableFuture<Result<State>> keepAlive(KeepAliveSessionSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }


    public CompletableFuture<Status> delete(DeleteSessionSettings settings) {
        throw new UnsupportedOperationException("Not supported for implicit sessions");
    }
}
