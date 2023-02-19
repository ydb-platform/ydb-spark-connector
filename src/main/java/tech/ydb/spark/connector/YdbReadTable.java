package tech.ydb.spark.connector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import tech.ydb.table.Session;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class YdbReadTable implements AutoCloseable {

    private final YdbScanOptions options;
    private final ArrayBlockingQueue<QueueItem> queue;
    private volatile State state;
    private volatile Exception firstIssue;
    private Session session;
    private ResultSetReader reader;

    public YdbReadTable(YdbScanOptions options, YdbInputPartition partition) {
        this.options = options;
        this.queue = new ArrayBlockingQueue<>(10);
        this.state = State.CREATED;
    }

    public void prepare() {
        if (state != State.CREATED)
            return;

        final ReadTableSettings.Builder sb = ReadTableSettings.newBuilder();
        int colcount = 0;
        scala.collection.Iterator<StructField> sfit = options.readSchema().seq().iterator();
        while (sfit.hasNext()) {
            sb.column(sfit.next().name());
            ++colcount;
        }
        if (colcount == 0)
            sb.column(options.getKeyColumns().get(0));
        if (! options.getRangeBegin().isEmpty())
            sb.fromKeyInclusive(makeRange(options.getRangeBegin()));
        if (! options.getRangeEnd().isEmpty())
            sb.toKeyInclusive(makeRange(options.getRangeEnd()));
        sb.timeout(Duration.ofHours(8));

        YdbConnector c = YdbRegistry.create(options.getCatalogName(), options.getConnectOptions());
        String tablePath = c.getDatabase() + "/" + options.getTableName();
        try {
            session = c.getTableClient().createSession(Duration.ofSeconds(10)).join().getValue();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        session.readTable(tablePath, sb.build(), rs -> {
                            queue.add(new QueueItem(rs));
                        }).join().expectSuccess();
                    } catch(Exception ex) {
                        if (firstIssue!=null)
                            firstIssue = ex;
                    }
                    queue.add(EndOfScan);
                }
            });
            t.setDaemon(true);
            t.setName("YdbReadTable");
            t.start();
        } catch(Exception ex) {
            throw new RuntimeException("Failed to initiate table scan for " + tablePath, ex);
        }
    }

    public boolean next() {
        if (state!=State.PREPARED)
            return false;
        boolean retval = false;
        while (!retval) {
            if (reader!=null)
                retval = reader.next();
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
            reader = qi.reader;
        }
        return true;
    }

    public InternalRow get() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void close() {
        if (session!=null) {
            try { session.close(); } catch(Exception x) {}
            session = null;
        }
    }

    @SuppressWarnings("unchecked")
    private TupleValue makeRange(List<Object> v) {
        final List<Value> l = new ArrayList<>();
        for (int pos=0; pos<v.size(); ++pos) {
            
        }
        return TupleValue.of(l);
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
