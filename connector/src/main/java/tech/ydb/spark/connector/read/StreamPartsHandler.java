package tech.ydb.spark.connector.read;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import com.google.protobuf.ByteString;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.util.ArrowUtils;

import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.proto.ValueProtos;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.query.result.arrow.ApacheArrowQueryResultPart;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.result.impl.ProtoValueReaders;

/**
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
class StreamPartsHandler implements QueryStream.PartsHandler, GrpcReadStream.Observer<ReadTablePart> {
    private volatile Schema schema = null;
    private final StreamReader reader;

    StreamPartsHandler(StreamReader reader) {
        this.reader = reader;
    }

    @Override
    public void onNext(ReadTablePart part) {
        reader.onNextPart(new ProtoBufPart(part.getResultSetReader()));
    }

    @Override
    public void onNextPart(QueryResultPart part) {
        // nothing
    }

    @Override
    public void onNextRawPart(long index, ValueProtos.ResultSet rs) {
        if (!rs.hasArrowFormatMeta()) {
            // use the protobuf part
            reader.onNextPart(new ProtoBufPart(ProtoValueReaders.forResultSet(rs)));
            return;
        }

        if (schema == null) {
            try {
               schema = readApacheArrowSchema(rs.getArrowFormatMeta().getSchema());
            } catch (IOException ex) {
                reader.onComplete(null, ex);
                return;
            }
        }

        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, ArrowUtils.rootAllocator());
        try {
            loadApacheArrowVector(vsr, rs.getData());
            ApacheArrowQueryResultPart part = new ApacheArrowQueryResultPart(index, vsr, rs.getColumnsList(),
                    rs.getTruncated());
            reader.onNextPart(new ArrowPart(vsr, part.getResultSetReader()));
        } catch (IOException | RuntimeException ex) {
            vsr.close();
            reader.onComplete(null, ex);
        }
    }

    protected VectorLoader createLoader(VectorSchemaRoot vsr) {
        return new VectorLoader(vsr);
    }

    private void loadApacheArrowVector(VectorSchemaRoot vsr, ByteString bytes) throws IOException {
        try (InputStream is = bytes.newInput()) {
            try (ReadChannel c = new ReadChannel(Channels.newChannel(is))) {
                try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(c, ArrowUtils.rootAllocator())) {
                    VectorLoader loader = createLoader(vsr);
                    loader.load(batch);
                }
            }
        }
    }

    private static Schema readApacheArrowSchema(ByteString bytes) throws IOException {
        try (InputStream is = bytes.newInput()) {
            try (ReadChannel channel = new ReadChannel(Channels.newChannel(is))) {
                return MessageSerializer.deserializeSchema(channel);
            }
        }
    }

    private class ProtoBufPart implements StreamPart {
        private final ResultSetReader rsr;

        ProtoBufPart(ResultSetReader rsr) {
            this.rsr = rsr;
        }

        @Override
        public int getRowCount() {
            return rsr.getRowCount();
        }

        @Override
        public ValueReader getColumn(String name) {
            return rsr.getColumn(name);
        }

        @Override
        public boolean next() {
            return rsr.next();
        }

        @Override
        public void close() { }
    }

    private class ArrowPart extends ProtoBufPart {
        private final VectorSchemaRoot vsr;

        ArrowPart(VectorSchemaRoot vsr, ResultSetReader rsr) {
            super(rsr);
            this.vsr = vsr;
        }

        @Override
        public void close() {
            vsr.close();
        }
    }
}
