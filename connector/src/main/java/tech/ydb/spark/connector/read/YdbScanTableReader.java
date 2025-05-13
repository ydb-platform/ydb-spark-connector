package tech.ydb.spark.connector.read;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.spark.connector.YdbTable;

/**
 * YDB table or index scan implementation through the ReadTable call.
 *
 * @author zinal
 */
public final class YdbScanTableReader extends CachedReader {
    private static final Logger logger = LoggerFactory.getLogger(CachedReader.class);

    private final QueryStream stream;

    public YdbScanTableReader(YdbTable table, YdbScanTableOptions options, String tabletId) {
        super(table, options.getTypes(), options.getQueueMaxSize(), options.getReadSchema());

        StringBuilder sb = new StringBuilder("SELECT");
        char dep = ' ';
        for (String name: outColumns) {
            sb.append(dep);
            sb.append('`');
            sb.append(name);
            sb.append('`');
            dep = ',';
        }
        sb.append(" FROM `").append(tablePath).append("`");

        if (tabletId != null) {
            sb.append(" WITH TabletId='").append(tabletId).append("'");
        }

        if (options.getRowLimit() > 0) {
            sb.append(" LIMIT ").append(options.getRowLimit());
        }

        String query = sb.toString();
        logger.debug("Execute scan query[{}]", query);

        Result<QuerySession> session = table.getCtx().getExecutor().createQuerySession();
        if (!session.isSuccess()) {
            logger.error("Cannot get session from the pool {}", session.getStatus());
            this.stream = null;
            return;
        }

        this.stream = session.getValue().createQuery(query, TxMode.SNAPSHOT_RO);
        this.stream.execute(this::onNextPart).whenComplete(this::onComplete).thenRun(() -> {
            session.getValue().close();
        });
    }

    private void onNextPart(QueryResultPart part) {
        super.onNextPart(part.getResultSetReader());
    }

    private void onComplete(Result<QueryInfo> result, Throwable th) {
        super.onComplete(result.getStatus(), th);
    }

    @Override
    protected void cancel() {
        if (stream != null) {
            stream.cancel();
        }
    }
}
