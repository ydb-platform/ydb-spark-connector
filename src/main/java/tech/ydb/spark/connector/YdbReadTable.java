package tech.ydb.spark.connector;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConversions;

import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.table.Session;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class YdbReadTable implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbReadTable.class);

    private final YdbScanOptions options;
    private final ArrayBlockingQueue<QueueItem> queue;
    private String tablePath;
    private Thread worker;
    private volatile State state;
    private volatile Exception firstIssue;
    private volatile Session session;
    private volatile GrpcReadStream<ReadTablePart> stream;
    private ResultSetReader current;

    public YdbReadTable(YdbScanOptions options) {
        this.options = options;
        this.queue = new ArrayBlockingQueue<>(100);
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
        sb.withRequestTimeout(Duration.ofHours(8));

        // Create or acquire the connector object.
        YdbConnector c = YdbRegistry.create(options.getCatalogName(), options.getConnectOptions());
        // The full table path is needed.
        // TODO: detect and convert the index pseudo-tables.
        tablePath = c.getDatabase() + "/" + options.getTableName();
        // TODO: add setting for the maximum session creation duration.
        session = c.getTableClient().createSession(Duration.ofSeconds(30)).join().getValue();
        try {
            // Opening the stream - which can be canceled.
            stream = session.executeReadTable(tablePath, sb.build());
            current = null;
            state = State.PREPARED;
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        stream.start(part -> {
                            while (true) {
                                try {
                                    queue.add(new QueueItem(part.getResultSetReader()));
                                    return;
                                } catch(IllegalStateException ise) {
                                    try { Thread.sleep(123L); } catch(InterruptedException ix) {}
                                }
                            }
                        }).join().expectSuccess();
                        queue.add(EndOfScan);
                    } catch(Exception ex) {
                        LOG.warn("Background scan failed for table {}", tablePath, ex);
                        synchronized(YdbReadTable.this) {
                            if (firstIssue==null)
                                firstIssue = ex;
                        }
                    }
                }
            });
            t.setDaemon(true);
            t.setName("YdbReadTable:"+tablePath);
            t.start();
            worker = t;
        } catch(Exception ex) {
            state = State.FAILED;
            LOG.warn("Failed to initiate scan for table {}", tablePath, ex);
            try {
                if (stream!=null)
                    stream.cancel();
            } catch(Exception tmp) {}
            try { session.close(); } catch(Exception tmp) {}
            throw new RuntimeException("Failed to initiate scan for table " + tablePath, ex);
        }
    }

    public boolean next() {
        if (state!=State.PREPARED)
            return false;
        while (true) {
            if (current!=null && current.next())
                return true; // have next row in the current block
            // end of rows or no block - need next
            QueueItem qi;
            while (true) {
                try {
                    qi = queue.take();
                    break;
                } catch(InterruptedException ix) {}
            }
            if (qi==null || qi.reader==null) {
                state = State.FINISHED;
                return false;
            }
            current = qi.reader;
        }
    }

    public InternalRow get() {
        final int count = current.getColumnCount();
        final ArrayList<Object> values = new ArrayList(count);
        for (int i=0; i<count; ++i) {
            values.add(convertFromYdb(current.getColumn(i)));
        }
        return InternalRow.fromSeq(JavaConversions.asScalaBuffer(values));
    }

    @Override
    public void close() {
        state = State.FINISHED;
        if (stream!=null) {
            try { stream.cancel(); } catch(Exception tmp) {}
        }
        if (worker!=null) {
            while (worker.isAlive()) {
                queue.clear();
                try { Thread.sleep(100L); } catch(InterruptedException ix) {}
            }
        }
        if (session!=null) {
            try { session.close(); } catch(Exception ex) {}
        }
        stream = null;
        session = null;
        worker = null;
        current = null;
    }

    @SuppressWarnings("unchecked")
    private TupleValue makeRange(List<Object> v) {
        final List<Value<?>> l = new ArrayList<>();
        for (Object x : v) {
            l.add(convertToYdb(x));
        }
        return TupleValue.of(l);
    }

    private static Value<?> convertToYdb(Object x) {
        if (x instanceof String) {
            return PrimitiveValue.newText(x.toString());
        }
        if (x instanceof UTF8String) {
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
        if (x instanceof Boolean) {
            return PrimitiveValue.newBool((Boolean)x);
        }
        if (x instanceof java.sql.Timestamp) {
            return PrimitiveValue.newTimestamp(((java.sql.Timestamp)x).toInstant());
        }
        if (x instanceof java.sql.Date) {
            return PrimitiveValue.newDate(((java.sql.Date)x).toLocalDate());
        }
        throw new IllegalArgumentException(x.getClass().getName());
    }

    private static Object convertFromYdb(ValueReader vr) {
        if (vr==null)
            return null;
        if (vr.getType().getKind().equals(Type.Kind.OPTIONAL)) {
            if (! vr.isOptionalItemPresent())
                return null;
        }
        Type t = vr.getType().unwrapOptional();
        switch (t.getKind()) {
            case PRIMITIVE:
                switch ((PrimitiveType)t) {
                    case Bool:
                        return vr.getBool();
                    case Bytes:
                        return vr.getBytes();
                    case Date:
                        return vr.getDate().toEpochDay();
                    case Datetime:
                        return vr.getDatetime().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
                    case Double:
                        return vr.getDouble();
                    case Float:
                        return vr.getFloat();
                    case Int16:
                        return vr.getInt16();
                    case Int32:
                        return vr.getInt32();
                    case Int64:
                        return vr.getInt64();
                    case Int8:
                        return vr.getInt8();
                    case Uint8:
                        return vr.getUint8();
                    case Json:
                        return UTF8String.fromString(vr.getJson());
                    case JsonDocument:
                        return UTF8String.fromString(vr.getJsonDocument());
                    case Text:
                        return UTF8String.fromString(vr.getText());
                    case Timestamp:
                        return vr.getTimestamp().toEpochMilli() * 1000L;
                    case Uint16:
                        return vr.getUint16();
                    case Uint32:
                        return vr.getUint32();
                    case Uint64:
                        return vr.getUint64();
                    case Uuid:
                        return vr.getUuid();
                    case Yson:
                        return vr.getYson();
                }
                break;
            case DECIMAL:
                return vr.getDecimal();
        }
        return null;
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
        FINISHED,
        FAILED
    }

}
