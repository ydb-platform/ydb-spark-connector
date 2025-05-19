package tech.ydb.spark.connector.read;

import java.time.Duration;
import java.util.stream.Collectors;

import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.KeysRange;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.TupleValue;

/**
 * YDB table or index scan implementation through the ReadTable call.
 *
 * @author zinal
 */
public final class YdbReadTableReader extends LazyReader {
    private final String id;
    private final GrpcReadStream<ReadTablePart> stream;

    public YdbReadTableReader(YdbTable table, YdbReadTableOptions options, KeysRange keysRange) {
        super(table, options.getTypes(), options.getMaxQueueSize(), options.getReadSchema());

        FieldInfo[] keys = table.getKeyColumns();
        ReadTableSettings.Builder rtsb = ReadTableSettings.newBuilder()
                // TODO: add setting for the maximum scan duration.
                .withRequestTimeout(Duration.ofHours(8))
                .orderedRead(true)
                .columns(outColumns);

        if (keysRange.hasFromValue()) {
            TupleValue tv = keysRange.readFromValue(types, keys);
            if (keysRange.includesFromValue()) {
                rtsb.fromKeyInclusive(tv);
            } else {
                rtsb.fromKeyExclusive(tv);
            }
        }
        if (keysRange.hasToValue()) {
            TupleValue tv = keysRange.readToValue(types, keys);
            if (keysRange.includesToValue()) {
                rtsb.toKeyInclusive(tv);
            } else {
                rtsb.toKeyExclusive(tv);
            }
        }

        int rowLimit = options.getRowLimit();
        if (rowLimit > 0) {
            rtsb.rowLimit(options.getRowLimit());
        }

        String columns = outColumns.stream().collect(Collectors.joining(","));
        this.id = "READ " + columns + " FROM " + tablePath + " RANGE " + keysRange + " LIMIT " + rowLimit;

        // Execute read table
        this.stream = table.getCtx().getExecutor().executeReadTable(tablePath, rtsb.build());
    }

    @Override
    protected String start() {
        stream.start(part -> onNextPart(part.getResultSetReader())).whenComplete(this::onComplete);
        return id;
    }

    @Override
    protected void cancel() {
        stream.cancel();
    }
}
