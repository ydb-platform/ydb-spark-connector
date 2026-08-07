package tech.ydb.spark.connector.write;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.impl.SparkSessionRetryContext;

/**
 * Common batching/retry/back-pressure scaffolding shared by all YDB data writers.
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public class YdbDataWriter implements DataWriter<InternalRow> {
    private static final Logger logger = LoggerFactory.getLogger(YdbDataWriter.class);

    private final SparkSessionRetryContext retryCtx;
    private final YdbWriter writer;
    private final int maxConcurrency;
    private final Semaphore semaphore;
    private final Metrics metrics = new Metrics();

    private volatile Status lastError = null;

    YdbDataWriter(SparkSessionRetryContext retryCtx, YdbWriter writer, int concurrency) {
        this.retryCtx = retryCtx;
        this.writer = writer;
        this.maxConcurrency = concurrency;
        this.semaphore = new Semaphore(maxConcurrency);
        logger.debug("created YDB data writer {}", writer);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        Status localError = lastError;
        if (localError != null) {
            logger.warn("ydb writer got error {} on write", localError);
            localError.expectSuccess("Cannot execute write");
        }

        writer.appendRow(record);
        if (writer.needToFlush()) {
            flushBatch();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        flushBatch();

        semaphore.acquireUninterruptibly(maxConcurrency);
        semaphore.release(maxConcurrency);

        Status localError = lastError;
        if (localError != null) {
            logger.error("ydb writer got error on commit: {}", localError);
            localError.expectSuccess("cannot commit write");
        }

        metrics.complete();
        // All rows have been written successfully
        return new YdbWriteCommit();
    }

    @Override
    public void abort() throws IOException {
        semaphore.acquireUninterruptibly(maxConcurrency);
        semaphore.release(maxConcurrency);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    private void flushBatch() {
        YdbWriter.Batch batch = writer.buildNextBatch();
        if (batch == null) {
            return;
        }

        semaphore.acquireUninterruptibly();
        if (lastError != null) {
            semaphore.release();
            return;
        }

        int rows = batch.rowsCount();
        int batchBytesSize = batch.bytesSize();
        OutputMetrics output = TaskContext.get().taskMetrics().outputMetrics();

        long started = System.currentTimeMillis();
        retryCtx.supplyStatus(batch).whenComplete((st, th) -> {
            if (st != null && st.isSuccess()) {
                metrics.measure(output, started, rows, batchBytesSize);
            } else {
                lastError = st != null ? st : Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th);
            }

            semaphore.release();
        });
    }

    private static class Metrics {
        private final AtomicLong count = new AtomicLong(0);
        private final LongAdder latency = new LongAdder();
        private final LongAdder rowsCount = new LongAdder();
        private final LongAdder byteSize = new LongAdder();

        public void measure(OutputMetrics output, long startedAt, long batchRows, long batchSize) {
            count.incrementAndGet();
            latency.add(System.currentTimeMillis() - startedAt);
            byteSize.add(batchSize);
            rowsCount.add(batchRows);

            output.setBytesWritten(byteSize.sum());
            output.setRecordsWritten(rowsCount.sum());
        }

        public void complete() {
            long avg = count.get() > 0 ? latency.sum() / count.get() : 0;
            logger.debug("written {} batches with {} rows and {} total byte size, avg latency {} ms",
                    count.get(), rowsCount.sum(), byteSize.sum(), avg);
        }
    }
}
