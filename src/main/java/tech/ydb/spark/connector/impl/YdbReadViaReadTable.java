package tech.ydb.spark.connector.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.spark.connector.YdbFieldType;
import tech.ydb.spark.connector.YdbKeyRange;
import tech.ydb.spark.connector.YdbScanOptions;
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
public class YdbReadViaReadTable extends YdbReadAbstract {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbReadViaReadTable.class);

    private static final QueueItem END_OF_SCAN = new QueueItem(null);

    private final ArrayBlockingQueue<QueueItem> queue;
    private Thread worker;
    private volatile Session session;
    private volatile GrpcReadStream<ReadTablePart> stream;

    public YdbReadViaReadTable(YdbScanOptions options, YdbKeyRange keyRange) {
        super(options, keyRange);
        this.queue = new ArrayBlockingQueue<>(options.getScanQueueDepth());
    }

    @Override
    protected void prepareScan(YdbConnector yc) {
        // Configuring settings for the table scan.
        final ReadTableSettings.Builder rtsb = ReadTableSettings.newBuilder();
        // Add all required fields.
        for (String colname : outColumns) {
            rtsb.column(colname);
        }
        configureRanges(rtsb);
        if (options.getRowLimit() > 0) {
            LOG.debug("Setting row limit to {}", options.getRowLimit());
            rtsb.rowLimit(options.getRowLimit());
        }
        // TODO: add setting for the maximum scan duration.
        rtsb.withRequestTimeout(Duration.ofHours(8));
        // Obtain the session (will be a long running one).
        session = yc.getTableClient().createSession(
                Duration.ofSeconds(options.getScanSessionSeconds())).join().getValue();
        try {
            // Opening the stream - which can be canceled.
            stream = session.executeReadTable(tablePath, rtsb.build());
            Thread t = new Thread(new Worker());
            t.setDaemon(true);
            t.setName("YdbReadTable:" + tablePath);
            t.start();
            worker = t;
        } catch (Exception ex) {
            setIssue(ex);
            LOG.warn("Failed to initiate scan for table {}", tablePath, ex);
            try {
                if (stream != null) {
                    stream.cancel();
                }
            } catch (Exception tmp) {
            }
            try {
                session.close();
            } catch (Exception tmp) {
            }
            throw new RuntimeException("Failed to initiate scan for table " + tablePath, ex);
        }
    }

    @Override
    protected ResultSetReader nextScan() {
        QueueItem qi;
        while (true) {
            try {
                qi = queue.take();
                break;
            } catch (InterruptedException ix) {
            }
        }
        if (qi == null) {
            return null;
        }
        return qi.reader;
    }

    @Override
    protected void closeScan() {
        if (stream != null) {
            try {
                stream.cancel();
            } catch (Exception tmp) {
            }
        }
        if (worker != null) {
            while (worker.isAlive()) {
                queue.clear();
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ix) {
                }
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (Exception ex) {
            }
        }
        stream = null;
        session = null;
        worker = null;
        current = null;
    }

    @SuppressWarnings("unchecked")
    private TupleValue makeRange(List<Object> values) {
        final List<YdbFieldType> keyTypes = options.getKeyTypes();
        final List<Value<?>> l = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); ++i) {
            Value<?> v = options.getTypes().convertToYdb(values.get(i), keyTypes.get(i));
            if (!v.getType().getKind().equals(Type.Kind.OPTIONAL)) {
                v = v.makeOptional();
            }
            l.add(v);
        }
        return TupleValue.of(l);
    }

    private void configureRanges(ReadTableSettings.Builder rtsb) {
        final YdbKeyRange.Limit realLeft = keyRange.getFrom();
        final YdbKeyRange.Limit realRight = keyRange.getTo();
        if (!realLeft.isUnrestricted()) {
            TupleValue tv = makeRange(realLeft.getValue());
            if (realLeft.isInclusive()) {
                rtsb.fromKeyInclusive(tv);
            } else {
                rtsb.fromKeyExclusive(tv);
            }
            LOG.debug("fromKey: {} -> {}", realLeft, tv);
        }
        if (!realRight.isUnrestricted()) {
            TupleValue tv = makeRange(realRight.getValue());
            if (realRight.isInclusive()) {
                rtsb.toKeyInclusive(tv);
            } else {
                rtsb.toKeyExclusive(tv);
            }
            LOG.debug("toKey: {} -> {}", realRight, tv);
        }
    }

    private void putToQueue(QueueItem qi) {
        while (true) {
            try {
                queue.add(qi);
                break; // exit the "queue put" retry loop
            } catch (IllegalStateException ise) {
                // The unlikely case of thread interrupt should not prevent us
                // from putting an item into the queue
                try {
                    Thread.sleep(35L);
                } catch (InterruptedException ix) {
                }
            }
        }
    }

    static final class QueueItem {

        final ResultSetReader reader;

        QueueItem(ResultSetReader reader) {
            this.reader = reader;
        }
    }

    class Worker implements Runnable {

        @Override
        public void run() {
            LOG.debug("Started background scan for table {}, range {}", tablePath, keyRange);
            try {
                stream.start(part -> {
                    final ResultSetReader rsr = part.getResultSetReader();
                    putToQueue(new QueueItem(rsr));
                    LOG.debug("Added portion of {} rows for table {} to the queue.",
                            rsr.getRowCount(), tablePath);
                }).join().expectSuccess();
            } catch (Exception ex) {
                boolean needReport = true;
                if (ex instanceof UnexpectedResultException) {
                    UnexpectedResultException ure = (UnexpectedResultException) ex;
                    if (ure.getStatus().getCode() == StatusCode.CLIENT_CANCELLED) {
                        needReport = false;
                    }
                }
                if (needReport) {
                    LOG.warn("Background scan failed for table {}, range {}", tablePath, keyRange, ex);
                    setIssue(ex);
                }
            }
            putToQueue(END_OF_SCAN);
            LOG.debug("Completed background scan for table {}, range {}", tablePath, keyRange);
        }
    }

}
