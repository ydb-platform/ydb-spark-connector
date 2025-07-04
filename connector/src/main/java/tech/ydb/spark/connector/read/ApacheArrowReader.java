package tech.ydb.spark.connector.read;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.proto.ValueProtos;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.settings.QueryResultFormat;
import tech.ydb.spark.connector.YdbContext;
import tech.ydb.table.query.Params;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class ApacheArrowReader implements PartitionReader<ColumnarBatch> {
    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private final YdbContext ctx;
    private final String query;
    private final Params params;

    private final ArrayBlockingQueue<ColumnarBatch> queue;

    private final AtomicLong readedRows = new AtomicLong();

    private volatile String id = null;
    private volatile long startedAt = System.currentTimeMillis();
    private volatile ColumnarBatch nextItem = null;
    private volatile Status finishStatus = null;
    private volatile QueryStream stream = null;

    public ApacheArrowReader(YdbContext ctx, SelectQuery query, int maxQueueSize) {
        this.ctx = ctx;
        this.query = query.toQuery();
        this.params = query.toQueryParams();
        this.queue = new ArrayBlockingQueue<>(maxQueueSize);
    }

    protected String start() {
        Result<QuerySession> session = ctx.getExecutor().createQuerySession();
        if (!session.isSuccess()) {
            onComplete(session.getStatus(), null);
        }

        ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                .withResultFormat(QueryResultFormat.APACHE_ARROW)
                .build();
        stream = session.getValue().createQuery(query, TxMode.SNAPSHOT_RO, params, settings);
        stream.execute(new QueryStream.PartsHandler() {
            @Override
            public void onNextPart(QueryResultPart part) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void onNextPartRaw(long index, ValueProtos.ResultSet rs) {
                try {
                    ByteString schema = rs.getArrowBatchSettings().getSchema();
                    ByteString batch = rs.getData();
                    onNextApacheArrowPart(schema, batch);
                } catch (Exception ex) {
                    logger.warn("[{}] processing got exception", id, ex);
                    onComplete(null, ex);
                }
            }
        }).whenComplete((res, th) -> {
            session.getValue().close();
            onComplete(res.getStatus(), th);
        });

        StringBuilder sb = new StringBuilder("ARROW ").append(query);
        params.values().forEach((k, v) -> {
            sb.append(", ").append(k).append("=").append(v);
        });
        return sb.toString();
    }

    protected void cancel() {
        if (stream != null) {
            stream.cancel();
        }
    }

    protected void onComplete(Status status, Throwable th) {
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
        logger.info("[{}] got {} rows in {} ms", id, readedRows.get(), ms);
    }

    private Schema readApacheArrowSchema(ByteString bytes) {
        try (InputStream is = bytes.newInput()) {
            try (ReadChannel channel = new ReadChannel(Channels.newChannel(is))) {
              return MessageSerializer.deserializeSchema(channel);
            }
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
    }

    protected void onNextApacheArrowPart(ByteString schemaBytes, ByteString batchBytes) throws Exception {
        RootAllocator allocator = ArrowUtils.rootAllocator();
        Schema schema = readApacheArrowSchema(schemaBytes);
        VectorSchemaRoot vector = VectorSchemaRoot.create(schema, allocator);
        try (InputStream is = batchBytes.newInput()) {
            try (ReadChannel channel = new ReadChannel(Channels.newChannel(is))) {
                try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(channel, allocator)) {
                    VectorLoader loader = new VectorLoader(vector);
                    loader.load(batch);
                }
            }
        }

        ArrowColumnVector[] columns = new ArrowColumnVector[schema.getFields().size()];
        int idx = 0;
        for (Field f : schema.getFields()) {
            columns[idx++] = new ArrowColumnVector(vector.getVector(f));
        }

        ColumnarBatch newItem = new ColumnarBatch(columns, vector.getRowCount());
        while (finishStatus == null) {
            if (queue.offer(newItem, 100, TimeUnit.MILLISECONDS)) {
                readedRows.addAndGet(vector.getRowCount());
                return;
            }
        }
    }

    @Override
    public boolean next() {
        if (id == null) {
            startedAt = System.currentTimeMillis();
            id = start();
            logger.debug("[{}] started, {} total", id, COUNTER.incrementAndGet());
        }
        while (true) {
            if (finishStatus != null) {
                finishStatus.expectSuccess("Scan failed.");
                if (nextItem == null && queue.isEmpty()) {
                    return false;
                }
            }

            if (nextItem != null) {
                return true;
            }

            try {
                nextItem = queue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Reading was interrupted", e);
            }
        }
    }

    @Override
    public ColumnarBatch get() {
        ColumnarBatch next = nextItem;
        if (next == null) {
            throw new IllegalStateException("Nothing to read");
        }
        nextItem = null;
        return next;
    }

    @Override
    public void close() {
        if (finishStatus == null) {
            cancel();
        }
    }
}

