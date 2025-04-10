package tech.ydb.spark.connector.impl;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.KeysRange;
import tech.ydb.spark.connector.read.YdbScanOptions;
import tech.ydb.table.Session;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB table or index scan implementation through the ReadTable call.
 *
 * @author zinal
 */
public class YdbScanReadTable implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(YdbScanReadTable.class);

    private final String tablePath;
    private final YdbTypes types;

    private final List<String> outColumns;
    private final GrpcReadStream<ReadTablePart> stream;
    private final CompletableFuture<Status> readStatus;

    private final ArrayBlockingQueue<QueueItem> queue;
    private volatile QueueItem currentItem = null;

    public YdbScanReadTable(YdbTable table, YdbScanOptions options, KeysRange keysRange) {
        this.tablePath = table.getTablePath();
        this.types = options.getTypes();
        this.queue = new ArrayBlockingQueue<>(options.getScanQueueDepth());

        FieldInfo[] keys = table.getKeyColumns();
        logger.debug("Configuring scan for table {}, range {}, columns {}", tablePath, keysRange, keys);
        ReadTableSettings.Builder rtsb = ReadTableSettings.newBuilder();
        scala.collection.Iterator<StructField> sfit = options.getReadSchema().toIterator();
        if (sfit.isEmpty()) {
            // In case no fields are required, add the first field of the primary key.
            rtsb.column(keys[0].getName());
        } else {
            while (sfit.hasNext()) {
                rtsb.column(sfit.next().name());
            }
        }

        final KeysRange.Limit realLeft = keysRange.getFrom();
        final KeysRange.Limit realRight = keysRange.getTo();
        if (!realLeft.isUnrestricted()) {
            TupleValue tv = makeRange(types, keys, realLeft.getValues());
            if (realLeft.isInclusive()) {
                rtsb.fromKeyInclusive(tv);
            } else {
                rtsb.fromKeyExclusive(tv);
            }
            logger.debug("fromKey: {} -> {}", realLeft, tv);
        }
        if (!realRight.isUnrestricted()) {
            TupleValue tv = makeRange(types, keys, realRight.getValues());
            if (realRight.isInclusive()) {
                rtsb.toKeyInclusive(tv);
            } else {
                rtsb.toKeyExclusive(tv);
            }
            logger.debug("toKey: {} -> {}", realRight, tv);
        }

        if (options.getRowLimit() > 0) {
            logger.debug("Setting row limit to {}", options.getRowLimit());
            rtsb.rowLimit(options.getRowLimit());
        }

        // TODO: add setting for the maximum scan duration.
        rtsb.withRequestTimeout(Duration.ofHours(8));

        ReadTableSettings settings = rtsb.build();
        this.outColumns = settings.getColumns();

        // Create or acquire the connector object.
        Result<Session> session = table.getCtx().getExecutor().createSession();
        if (!session.isSuccess()) {
            this.stream = null;
            this.readStatus = CompletableFuture.completedFuture(session.getStatus());
            return;
        }

        this.stream = session.getValue().executeReadTable(table.getTablePath(), settings);
        this.readStatus = this.stream.start(this::onNextPart);
        // Read table is not using session, so we can close it
        session.getValue().close();
    }

    private static TupleValue makeRange(YdbTypes types, FieldInfo[] keys, Serializable[] values) {
        final List<Value<?>> l = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; ++i) {
            Value<?> v = types.convertToYdb(values[i], keys[i].getType());
            if (!v.getType().getKind().equals(Type.Kind.OPTIONAL)) {
                v = v.makeOptional();
            }
            l.add(v);
        }
        return TupleValue.of(l);
    }
    private void onNextPart(ReadTablePart part) {
        QueueItem nextItem = new QueueItem(part.getResultSetReader());
        try {
            while (!readStatus.isDone()) {
                if (queue.offer(nextItem, 100, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException ex) {
            logger.warn("Scan read of table {} was interrupted", tablePath);
            Thread.currentThread().interrupt();
        }
    }

    public boolean next() {
        while (true) {
            if (readStatus.isDone()) {
                readStatus.join().expectSuccess("Scan failed.");
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

    public InternalRow get() {
        if (currentItem == null) {
            throw new IllegalStateException("Nothing to read");
        }
        return currentItem.get();
    }

    @Override
    public void close() {
        if (!readStatus.isDone()) {
            stream.cancel();
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
}
