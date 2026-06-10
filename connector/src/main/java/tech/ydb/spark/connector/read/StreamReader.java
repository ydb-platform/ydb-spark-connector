package tech.ydb.spark.connector.read;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcFlowControl;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.OperationOption;

/**
 *
 * @author Aleksandr Gorshenin
 */
abstract class StreamReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private final String[] fieldNames;
    private final YdbTypes types;

    private final BlockingQueue<StreamPart> queue;
    private final AtomicLong readedRows = new AtomicLong();

    protected final GrpcFlowControl flowControl;

    private volatile String id = null;
    private volatile GrpcCall call = null;
    private volatile long startedAt = System.currentTimeMillis();
    private volatile StreamPart currentItem = null;
    private volatile Status finishStatus = null;

    protected StreamReader(YdbTypes types, int maxQueueSize, StructType schema) {
        this.fieldNames = schema.fieldNames();
        this.types = types;
        this.queue = new ArrayBlockingQueue<>(maxQueueSize);
        this.flowControl = (req) -> {
            call = new GrpcCall(req);
            return call;
        };
    }

    protected abstract String start();

    protected abstract void cancel();

    void onComplete(Status status, Throwable th) {
        long ms = System.currentTimeMillis() - startedAt;
        if (status != null) {
            if (!status.isSuccess()) {
                logger.warn("[{}] reading finished with error {}", id, status);
            }
            finishStatus = status;
        }
        if (th != null) {
            logger.error("[{}] reading finished with exception", id, th);
            finishStatus = Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th);
        }
        COUNTER.decrementAndGet();
        logger.debug("[{}] got {} rows in {} ms", id, readedRows.get(), ms);
    }

    void onNextPart(StreamPart part) {
        readedRows.addAndGet(part.getRowCount());
        queue.add(part);
    }

    @Override
    public boolean next() {
        if (id == null) {
            startedAt = System.currentTimeMillis();
            id = start();
            logger.trace("[{}] started, {} total", id, COUNTER.incrementAndGet());
        }

        while (true) {
            if (finishStatus != null) {
                finishStatus.expectSuccess("Scan failed.");
                if (currentItem == null && queue.isEmpty()) {
                    return false;
                }
            }

            if (currentItem != null) {
                if (currentItem.next()) {
                    return true;
                }
                currentItem.close();
                currentItem = null;
            }

            try {
                currentItem = queue.poll(100, TimeUnit.MILLISECONDS);
                if (currentItem != null) {
                    // call is never null if item has been read
                    call.requestNextMessage();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Reading was interrupted", e);
            }
        }
    }

    @Override
    public InternalRow get() {
        if (currentItem == null) {
            throw new IllegalStateException("Nothing to read");
        }

        if (fieldNames.length == 0) {
            return InternalRow.empty();
        }
        InternalRow row = new GenericInternalRow(fieldNames.length);
        for (int i = 0; i < fieldNames.length; ++i) {
            types.setRowValue(row, i, currentItem.getColumn(fieldNames[i]));
        }
        return row;
    }

    @Override
    public void close() {
        if (finishStatus == null) {
            cancel();
        }
        if (currentItem != null) {
            currentItem.close();
            currentItem = null;
        }
        for (StreamPart item = queue.poll(); item != null; item = queue.poll()) {
            item.close();
        }
    }

    private class GrpcCall implements GrpcFlowControl.Call {

        private final IntConsumer req;

        GrpcCall(IntConsumer req) {
            this.req = req;
        }

        @Override
        public void onStart() {
            req.accept(queue.remainingCapacity());
        }

        @Override
        public void onMessageRead() {
            // nothing
        }

        public void requestNextMessage() {
            req.accept(1);
        }
    }

    public static int readQueueMaxSize(CaseInsensitiveStringMap options) {
        try {
            int scanQueueDepth = OperationOption.READQUEUE_SIZE.readInt(options, 3);
            if (scanQueueDepth < 2) {
                logger.warn("Value of {} property too low, reverting to minimum of 2.", OperationOption.READQUEUE_SIZE);
                return 2;
            }

            return scanQueueDepth;
        } catch (NumberFormatException ex) {
            logger.warn("Illegal value of {} property, reverting to default of 3.", OperationOption.READQUEUE_SIZE, ex);
            return 3;
        }
    }
}
