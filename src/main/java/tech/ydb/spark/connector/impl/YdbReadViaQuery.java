package tech.ydb.spark.connector.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.spark.connector.YdbCatalog;
import tech.ydb.spark.connector.YdbFieldType;
import tech.ydb.spark.connector.YdbKeyRange;
import tech.ydb.spark.connector.YdbScanOptions;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB table or index scan implementation through the YQL query call.
 *
 * @author zinal
 */
public class YdbReadViaQuery extends YdbReadAbstract {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbReadViaQuery.class);

    private static final QueueItem END_OF_SCAN = new QueueItem(null);

    private final ArrayBlockingQueue<QueueItem> queue;
    private Thread worker;

    public YdbReadViaQuery(YdbScanOptions options, YdbKeyRange keyRange) {
        super(options, keyRange);
        this.queue = new ArrayBlockingQueue<>(options.getScanQueueDepth());
    }

    @Override
    protected void prepareScan(YdbConnector yc) {
        String sqlQuery = generateQuery();

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
        // maximum scan duration.
        if (options.getMaxDurationSeconds() > 0) {
            rtsb.withRequestTimeout(Duration.ofSeconds(options.getMaxDurationSeconds()));
        } else {
            rtsb.withRequestTimeout(Duration.ofHours(8));
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
        if (worker != null) {
            while (worker.isAlive()) {
                queue.clear();
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ix) {
                }
            }
        }
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

    private String generateQuery() {
        final StringBuilder sb = new StringBuilder(200);
        sb.append("SELECT ");
        appendColumns(sb);
        sb.append(" FROM ");
        appendTableName(sb);
        if (hasConditions()) {
            sb.append(" WHERE 1=1");
            appendRange(sb);
            appendPredicates(sb);
        }
        return sb.toString();
    }

    private void appendColumns(StringBuilder sb) {
        boolean comma = false;
        for (String colname : outColumns) {
            if (comma) {
                sb.append(", ");
            } else {
                comma = true;
            }
            sb.append("`").append(colname).append("`");
        }
    }

    private void appendTableName(StringBuilder sb) {
        if (tablePath.endsWith(YdbCatalog.IX_IMPL)) {
            int ixNameEnd = tablePath.length() - YdbCatalog.IX_IMPL.length();
            int ixNameStart = tablePath.lastIndexOf("/", ixNameEnd);
            if (ixNameStart < 0) {
                throw new IllegalArgumentException("Illegal index pathname: " + tablePath);
            }
            String ixName = tablePath.substring(ixNameStart + 1, ixNameEnd);
            String tabName = tablePath.substring(0, ixNameStart);
            sb.append("`").append(tabName).append("`");
            sb.append(" VIEW ");
            sb.append("`").append(ixName).append("`");
        } else {
            sb.append("`").append(tablePath).append("`");
        }
    }

    private boolean hasConditions() {
        if (keyRange != null && !keyRange.isUnrestricted()) {
            return true;
        }
        if (!options.getPredicates().isEmpty()) {
            return true;
        }
        return false;
    }

    private void appendRange(StringBuilder sb) {
        if (keyRange == null) {
            return;
        }
        if (!keyRange.getFrom().isUnrestricted()) {
            sb.append(" AND (");
            sb.append(")");
        }
        if (!keyRange.getTo().isUnrestricted()) {
            sb.append(" AND (");
            sb.append(")");
        }
    }

    private void appendPredicates(StringBuilder sb) {
        // TODO: implementation
        throw new UnsupportedOperationException("Not supported yet.");
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
                // TODO: implementation
                throw new UnsupportedOperationException("Not supported yet.");
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
