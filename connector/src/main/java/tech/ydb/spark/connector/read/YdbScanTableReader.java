package tech.ydb.spark.connector.read;


import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.spark.connector.YdbContext;
import tech.ydb.spark.connector.YdbTable;

/**
 * YDB table or index scan implementation through the ReadTable call.
 *
 * @author zinal
 */
public final class YdbScanTableReader extends LazyReader {
    private final YdbContext ctx;
    private final String query;
    private volatile QueryStream stream = null;

    public YdbScanTableReader(YdbTable table, YdbScanTableOptions options, String tabletId) {
        super(table, options.getTypes(), options.getQueueMaxSize(), options.getReadSchema());

        this.ctx = table.getCtx();

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

        this.query = sb.toString();
    }

    @Override
    protected String start() {
        Result<QuerySession> session = ctx.getExecutor().createQuerySession();
        if (!session.isSuccess()) {
            onComplete(session.getStatus(), null);
        }

        stream = session.getValue().createQuery(query, TxMode.SNAPSHOT_RO);
        stream.execute(part -> onNextPart(part.getResultSetReader())).whenComplete((res, th) -> {
            session.getValue().close();
            onComplete(res.getStatus(), th);
        });

        return query;
    }

    @Override
    protected void cancel() {
        if (stream != null) {
            stream.cancel();
        }
    }
}
