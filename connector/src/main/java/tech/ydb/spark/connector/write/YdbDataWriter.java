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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(YdbDataWriter.class);

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
        this.maxConcurrency = 2;
        this.semaphore = new Semaphore(maxConcurrency);
    }

    abstract CompletableFuture<Status> executeWrite(ListValue batch);

    @Override
    public void write(InternalRow record) throws IOException {
        if (lastError != null) {
            logger.warn("ydb writer got error {} on write", lastError);
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
            logger.warn("ydb writer got error {} on commit", lastError);
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
}
