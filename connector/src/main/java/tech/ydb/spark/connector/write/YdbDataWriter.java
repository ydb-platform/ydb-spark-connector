package tech.ydb.spark.connector.write;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

/**
 * YDB table writer: basic row writer.
 *
 * @author zinal
 */
public abstract class YdbDataWriter implements DataWriter<InternalRow> {

    private final YdbTypes types;
    private final StructType structType;
    private final ValueReader[] readers;

    private final int maxBatchSize;
    private final int maxConcurrency;
    private final Semaphore semaphore;

    private final Map<CompletableFuture<?>, CompletableFuture<?>> writesInFly = new ConcurrentHashMap<>();
    private List<Value<?>> currentBatch = new ArrayList<>();
    private volatile Status lastError = null;

    public YdbDataWriter(YdbTypes types, StructType structType, ValueReader[] readers, int maxBatchSize) {
        this.types = types;
        this.structType = structType;
        this.readers = readers;
        this.maxBatchSize = maxBatchSize;
        this.maxConcurrency = 5;
        this.semaphore = new Semaphore(maxConcurrency);
    }

    abstract CompletableFuture<Status> executeWrite(ListValue batch);

    @Override
    public void write(InternalRow record) throws IOException {
        if (lastError != null) {
            lastError.expectSuccess("Cannot execute write");
        }

        Value<?>[] row = new Value<?>[readers.length];
        for (int idx = 0; idx < row.length; ++idx) {
            row[idx] = readers[idx].read(types, record);
        }

        currentBatch.add(structType.newValueUnsafe(row));
        if (currentBatch.size() >= maxBatchSize) {
            writeBatch();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        writeBatch();

        semaphore.acquireUninterruptibly(maxConcurrency);
        semaphore.release(maxConcurrency);

        if (lastError != null) {
            lastError.expectSuccess("cannot commit write");
        }

        // All rows have been written successfully
        return new YdbWriteCommit();
    }

    @Override
    public void abort() throws IOException {
        writesInFly.keySet().forEach(f -> f.cancel(false));
        semaphore.acquireUninterruptibly(maxConcurrency);
        semaphore.release(maxConcurrency);
    }

    @Override
    public void close() throws IOException {
    }

    private void writeBatch() {
        if (currentBatch.isEmpty()) {
            return;
        }

        Value<?>[] copy = currentBatch.toArray(new Value<?>[0]);
        currentBatch = new ArrayList<>();

        semaphore.acquireUninterruptibly();
        if (lastError != null) {
            semaphore.release();
            return;
        }

        CompletableFuture<Status> future = executeWrite(ListValue.of(copy));
        writesInFly.put(future, future);

        future.whenComplete((st, th) -> {
            writesInFly.remove(future);

            if (st != null && !st.isSuccess()) {
                lastError = st;
            }
            if (th != null) {
                lastError = Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th);
            }
            semaphore.release();
        });
    }

/*
    private void startNewStatement(List<Value<?>> input) {
        LOG.debug("[{}, {}] Sending a batch of {} rows into table {}",
                partitionId, taskId, input.size(), tablePath);
        if (currentStatus != null) {
            currentStatus.join().expectSuccess();
            currentStatus = null;
            LOG.debug("[{}, {}] Previous async batch completed for table {}",
                    partitionId, taskId, tablePath);
        }
        // The list is being copied here.
        // DO NOT move this call into the async methods below.
        ListValue value = listType.newValue(input);
        if (IngestMethod.BULK_UPSERT.equals(ingestMethod)) {
            currentStatus = connector.getRetryCtx().supplyStatus(
                    session -> session.executeBulkUpsert(
                            tablePath, value, new BulkUpsertSettings()));
            LOG.debug("[{}, {}] Async bulk upsert started on table {}",
                    partitionId, taskId, tablePath);
        } else {
            currentStatus = connector.getRetryCtx().supplyStatus(
                    session -> session.executeDataQuery(sqlStatement,
                            TxControl.serializableRw().setCommitTx(true),
                            Params.of("$input", value))
                            .thenApply(result -> result.getStatus()));
            LOG.debug("[{}, {}] Async upsert transaction started on table {}",
                    partitionId, taskId, tablePath);
        }
    }
*/
//    private static List<FieldInfo> makeStatementFields(YdbTypes types, YdbWriteOptions options,
//            List<StructField> inputFields) {
//        final List<FieldInfo> out = new ArrayList<>(inputFields.size());
////        if (options.isMapByNames()) {
////            for (StructField sf : inputFields) {
////                final String name = sf.name();
////                FieldInfo yfi = options.getFieldsMap().get(name);
////                if (yfi != null) {
////                    out.add(yfi);
////                }
////            }
////        } else {
//            for (int pos = 0; pos < inputFields.size(); ++pos) {
//                out.add(FieldInfo.fromSchema(types, inputFields.get(pos)));
//            }
////        }
//        if (options.getAddtitionalPk() != null) {
//            // Generated PK is the last column, if one is presented at all.
//            out.add(new FieldInfo(options.getAddtitionalPk(), FieldType.Text, false));
//        }
//        return out;
//    }
//
//    private static StructType makeInputType(List<FieldInfo> statementFields) {
//        if (statementFields.isEmpty()) {
//            throw new IllegalArgumentException("Empty input field list specified for writing");
//        }
//        final Map<String, Type> m = new HashMap<>();
//        for (FieldInfo yfi : statementFields) {
//            m.put(yfi.getName(), FieldType.toSdkType(yfi.getType(), yfi.isNullable()));
//        }
//        return tech.ydb.StructType.of(m);
//    }

}
