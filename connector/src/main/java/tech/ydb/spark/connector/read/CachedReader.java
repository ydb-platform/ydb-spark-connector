package tech.ydb.spark.connector.read;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class CachedReader implements PartitionReader<InternalRow> {
    private static final Logger logger = LoggerFactory.getLogger(CachedReader.class);

    protected final String tablePath;
    protected final List<String> outColumns;
    protected final YdbTypes types;

    private final ArrayBlockingQueue<QueueItem> queue;
    private volatile QueueItem currentItem = null;
    private volatile Status finishStatus = null;

    public CachedReader(YdbTable table, YdbTypes types, int maxQueueSize, StructType schema) {
        this.tablePath = table.getTablePath();
        this.types = types;
        this.queue = new ArrayBlockingQueue<>(maxQueueSize);
        this.outColumns = new ArrayList<>(Arrays.asList(schema.fieldNames()));

        if (outColumns.isEmpty()) {
            outColumns.add(table.getKeyColumns()[0].getName());
        }
    }

    protected abstract void cancel();

    protected void onComplete(Status status, Throwable th) {
        if (status != null) {
            if (!status.isSuccess()) {
                logger.warn("read table {} finished with error {}", tablePath, status);
            }
            finishStatus = status;
        }
        if (th != null) {
            logger.error("read table {} finished with exception", tablePath, th);
            finishStatus = Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th);
        }
    }

    protected void onNextPart(ResultSetReader reader) {
        QueueItem nextItem = new QueueItem(reader);
        try {
            while (finishStatus == null) {
                if (queue.offer(nextItem, 100, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException ex) {
            logger.warn("Scan read of table {} was interrupted", tablePath);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean next() {
        while (true) {
            if (finishStatus != null) {
                finishStatus.expectSuccess("Scan failed.");
                if (currentItem == null && queue.isEmpty()) {
                    return false;
                }
            }

            if (currentItem != null && currentItem.reader.next()) {
                return true;
            }

            try {
                currentItem = queue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Scan was interrupted", e);
            }
        }
    }

    @Override
    public InternalRow get() {
        if (currentItem == null) {
            throw new IllegalStateException("Nothing to read");
        }
        return currentItem.get();
    }

    @Override
    public void close() {
        if (finishStatus == null) {
            cancel();
        }
    }

    private class QueueItem {
        final ResultSetReader reader;
        final int[] columnIndexes;

        QueueItem(ResultSetReader reader) {
            this.reader = reader;
            this.columnIndexes = new int[outColumns.size()];
            int idx = 0;
            for (String column: outColumns) {
                columnIndexes[idx++] = reader.getColumnIndex(column);
            }
        }

        public InternalRow get() {
            InternalRow row = new GenericInternalRow(columnIndexes.length);
            for (int i = 0; i < columnIndexes.length; ++i) {
                types.setRowValue(row, i, reader.getColumn(columnIndexes[i]));
            }
            return row;
        }
    }

    public static int readQueueMaxSize(CaseInsensitiveStringMap options) {
        try {
            int scanQueueDepth = OperationOption.SCAN_QUEUE_DEPTH.readInt(options, 3);
            if (scanQueueDepth < 2) {
                logger.warn("Value of {} property too low, reverting to minimum of 2.",
                        OperationOption.SCAN_QUEUE_DEPTH);
                return 2;
            }

            return scanQueueDepth;
        } catch (NumberFormatException nfe) {
            logger.warn("Illegal value of {} property, reverting to default of 3.",
                        OperationOption.SCAN_QUEUE_DEPTH, nfe);
            return 3;
        }
    }
}
