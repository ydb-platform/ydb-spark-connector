package tech.ydb.spark.connector.read;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.result.ResultSetReader;

/**
 * YDB table or index scan implementation through the ReadTable call.
 *
 * @author zinal
 */
public class YdbScanTableReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(YdbScanTableReader.class);

    private final String tablePath;
    private final YdbTypes types;

    private final List<String> outColumns;
    private final QueryStream stream;
    private final CompletableFuture<Status>  readStatus;

    private final ArrayBlockingQueue<QueueItem> queue;

    private volatile QueueItem currentItem = null;

    public YdbScanTableReader(YdbTable table, YdbScanTableOptions options, int tablet) {
        this.tablePath = table.getTablePath();
        this.types = options.getTypes();
        this.queue = new ArrayBlockingQueue<>(options.getQueueMaxSize());
        this.outColumns = new ArrayList<>(Arrays.asList(options.getReadSchema().fieldNames()));

        if (outColumns.isEmpty()) {
            outColumns.add(table.getKeyColumns()[0].getName());
        }

        StringBuilder sb = new StringBuilder("SELECT");
        char dep = ' ';
        for (String name: outColumns) {
            sb.append(dep);
            sb.append('`');
            sb.append(name);
            sb.append('`');
            dep = ',';
        }
        sb.append(" FROM `").append(tablePath).append("`");
        String query = sb.toString();

        logger.debug("Execute scan query[{}]", query);

        Result<QuerySession> session = table.getCtx().getExecutor().createQuerySession();
        if (!session.isSuccess()) {
            logger.error("Cannot get session from the pool {}", session.getStatus());
            this.stream = null;
            this.readStatus = CompletableFuture.completedFuture(session.getStatus());
            return;
        }

        this.stream = session.getValue().createQuery(query, TxMode.SNAPSHOT_RO);
        this.readStatus = this.stream.execute(this::onNextPart).thenApply(Result::getStatus);
        this.readStatus.whenComplete((status, th) -> {
            if (status != null && !status.isSuccess()) {
                logger.warn("read table {} finished with error {}", table.getTablePath(), status);
            }
            if (th != null) {
                logger.error("read table {} finished with exception", table.getTablePath(), th);
            }
            session.getValue().close();
        });
    }

    private void onNextPart(QueryResultPart part) {
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

    @Override
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

    @Override
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
