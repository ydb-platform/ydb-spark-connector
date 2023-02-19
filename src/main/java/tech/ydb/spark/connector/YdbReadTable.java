package tech.ydb.spark.connector;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import tech.ydb.table.Session;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.TupleValue;

/**
 *
 * @author zinal
 */
public class YdbReadTable implements AutoCloseable {

    private final YdbScanOptions options;
    private final ArrayBlockingQueue<QueueItem> queue;
    private Session session;
    private ResultSetReader reader;

    public YdbReadTable(YdbScanOptions options, YdbInputPartition partition) {
        this.queue = new ArrayBlockingQueue<QueueItem>(10);
        this.options = options;
    }

    public void prepare() {
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
        session = c.getTableClient().createSession(Duration.ofSeconds(10)).join().getValue();
        try {
            // TODO: async run, handle completion
            session.readTable(tablePath, sb.build(), rs -> {
                queue.add(new QueueItem(rs));
            });
        } catch(Exception ex) {
            throw new RuntimeException("Failed to initiate table scan for ", ex);
        }
    }

    public boolean next() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void close() {
        if (session!=null) {
            try { session.close(); } catch(Exception x) {}
            session = null;
        }
    }

    public InternalRow get() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    private TupleValue makeRange(List<Object> rangeBegin) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    static class QueueItem {
        final ResultSetReader reader;
        public QueueItem(ResultSetReader reader) {
            this.reader = reader;
        }
    }

    static final QueueItem EndOfScan = new QueueItem(null);

}
