package tech.ydb.spark.connector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import tech.ydb.table.Session;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class YdbReadTable implements AutoCloseable {

    private final YdbScanOptions options;
    private final ArrayBlockingQueue<QueueItem> queue;
    private String tablePath;
    private Thread worker;
    private volatile State state;
    private volatile Exception firstIssue;
    private ResultSetReader current;

    public YdbReadTable(YdbScanOptions options, YdbInputPartition partition) {
        this.options = options;
        this.queue = new ArrayBlockingQueue<>(10);
        this.state = State.CREATED;
    }

    public void prepare() {
        if (state != State.CREATED)
            return;

        // Configuring settings for the table scan.
        final ReadTableSettings.Builder sb = ReadTableSettings.newBuilder();
        // Add all required fields.
        int colcount = 0;
        scala.collection.Iterator<StructField> sfit = options.readSchema().seq().iterator();
        while (sfit.hasNext()) {
            sb.column(sfit.next().name());
            ++colcount;
        }
        if (colcount == 0) {
            // In case no fields are required, add the first field of the primary key.
            sb.column(options.getKeyColumns().get(0));
        }
        if (! options.getRangeBegin().isEmpty()) {
            // Left scan limit.
            sb.fromKeyInclusive(makeRange(options.getRangeBegin()));
        }
        if (! options.getRangeEnd().isEmpty()) {
            // Right scan limit.
            sb.toKeyInclusive(makeRange(options.getRangeEnd()));
        }
        // TODO: add setting for the maximum scan duration.
        sb.timeout(Duration.ofHours(8));

        // Create or acquire the connector object.
        YdbConnector c = YdbRegistry.create(options.getCatalogName(), options.getConnectOptions());
        // The full table path is needed.
        // TODO: detect and convert the index pseudo-tables.
        tablePath = c.getDatabase() + "/" + options.getTableName();
        try {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    // TODO: add setting for the maximum session creation duration.
                    final Session session = c.getTableClient()
                            .createSession(Duration.ofSeconds(30)).join().getValue();
                    try {
                        session.readTable(tablePath, sb.build(), rs -> {
                            if (state!=State.PREPARED) {
                                throw new IllegalStateException();
                            }
                            queue.add(new QueueItem(rs));
                        }).join().expectSuccess();
                        queue.add(EndOfScan);
                    } catch(Exception ex) {
                        if (firstIssue!=null)
                            firstIssue = ex;
                    } finally {
                        try {
                            session.close();
                        } catch(Exception ignore) {}
                    }
                }
            });
            t.setDaemon(true);
            t.setName("YdbReadTable");
            t.start();
            worker = t;
        } catch(Exception ex) {
            throw new RuntimeException("Failed to initiate table scan for " + tablePath, ex);
        }
        state = State.PREPARED;
    }

    public boolean next() {
        if (state!=State.PREPARED)
            return false;
        boolean retval = false;
        while (!retval) {
            if (current!=null)
                retval = current.next();
            if (retval)
                break;
            QueueItem qi;
            while (true) {
                try {
                    qi = queue.take();
                    break;
                } catch(InterruptedException ix) {}
            }
            if (qi.reader == null) {
                state = State.FINISHED;
                return false;
            }
            current = qi.reader;
        }
        return true;
    }

    public InternalRow get() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void close() {
        state = State.FINISHED;
        if (worker!=null) {
            while (worker.isAlive()) {
                queue.clear();
                try { Thread.sleep(100L); } catch(InterruptedException ix) {}
            }
        }
    }

    @SuppressWarnings("unchecked")
    private TupleValue makeRange(List<Object> v) {
        final List<Value> l = new ArrayList<>();
        for (Object x : v) {
            l.add(convertToYdb(x));
        }
        return TupleValue.of(l);
    }

    private Value convertToYdb(Object x) {
        if (x instanceof String) {
            return PrimitiveValue.newText(x.toString());
        }
        if (x instanceof Integer) {
            return PrimitiveValue.newInt32((Integer)x);
        }
        if (x instanceof Long) {
            return PrimitiveValue.newInt64((Long)x);
        }
        if (x instanceof byte[]) {
            return PrimitiveValue.newBytes((byte[])x);
        }
        if (x instanceof Double) {
            return PrimitiveValue.newDouble((Double)x);
        }
        if (x instanceof Float) {
            return PrimitiveValue.newFloat((Float)x);
        }
        if (x instanceof BigDecimal) {
            return DecimalType.getDefault().newValue((BigDecimal)x);
        }
        throw new IllegalArgumentException(x.getClass().getName());
    }

    static class QueueItem {
        final ResultSetReader reader;
        public QueueItem(ResultSetReader reader) {
            this.reader = reader;
        }
    }

    static final QueueItem EndOfScan = new QueueItem(null);

    static enum State {
        CREATED,
        PREPARED,
        FINISHED
    }

}
