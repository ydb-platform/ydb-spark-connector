package tech.ydb.spark.connector.read;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class YdbReadTableReader extends CachedReader {
    private static final Logger logger = LoggerFactory.getLogger(YdbReadTableReader.class);
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

        if (options.getRowLimit() > 0) {
            rtsb.rowLimit(options.getRowLimit());
        }

        logger.debug("Configuring scan for table {} with range {} and limit {}, columns {}",
                tablePath, keysRange, options.getRowLimit(), keys);

        ReadTableSettings settings = rtsb.build();

        // Execute read table
        this.stream = table.getCtx().getExecutor().executeReadTable(table.getTablePath(), settings);
        this.stream.start(this::onNextPart).whenComplete(this::onComplete);
    }

    private void onNextPart(ReadTablePart part) {
        super.onNextPart(part.getResultSetReader());
    }

    @Override
    protected void cancel() {
        stream.cancel();
    }
}
