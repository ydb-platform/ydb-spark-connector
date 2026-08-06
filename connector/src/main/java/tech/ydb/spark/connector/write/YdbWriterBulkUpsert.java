package tech.ydb.spark.connector.write;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Status;
import tech.ydb.proto.ValueProtos;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.Session;
import tech.ydb.table.query.BulkUpsertData;
import tech.ydb.table.settings.BulkUpsertSettings;

class YdbWriterBulkUpsert extends YdbWriterProtobuf {
    private final String tablePath;
    private final BulkUpsertSettings settings = new BulkUpsertSettings();

    YdbWriterBulkUpsert(String tablePath, YdbTypes types, int maxRowsCount, int maxBytesSize, List<ColumnEntry> cols) {
        super(types, cols, maxRowsCount, maxBytesSize);
        this.tablePath = tablePath;
    }

    @Override
    public String toString() {
        return "YdbWriterBulkUpsert[" + tablePath + "]";
    }

    @Override
    protected CompletableFuture<Status> writeData(Session session, ValueProtos.TypedValue data) {
        return session.executeBulkUpsert(tablePath, new BulkUpsertData(data), settings);
    }
}
