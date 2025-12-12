package tech.ydb.spark.connector.read;



import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.spark.connector.YdbQueryTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.query.Params;

/**
 * Scan is the factory for the Batch.
 *
 * @author zinal
 */
public class YdbQueryScan implements Scan, Batch, ScanBuilder, PartitionReaderFactory {
    private static final Logger logger = LoggerFactory.getLogger(YdbReadTable.class);
    private static final long serialVersionUID = 9149595384600250661L;

    private final YdbQueryTable query;
    private final YdbTypes types;
    private final int queueMaxSize;

    public YdbQueryScan(YdbQueryTable query, CaseInsensitiveStringMap options) {
        this.query = query;
        this.types = new YdbTypes(options);

        this.queueMaxSize = StreamReader.readQueueMaxSize(options);
    }

    @Override
    public Scan build() {
        return this;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return this;
    }

    @Override
    public StructType readSchema() {
        return query.schema();
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return CustomYqlPartition.PLAN;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new QueryReader();
    }

    private final class QueryReader extends StreamReader {
        private volatile QueryStream stream = null;

        QueryReader() {
            super(types, queueMaxSize, query.schema());
        }

        @Override
        protected String start() {
            String yql = query.getQuery();
            Result<QuerySession> session = query.getCtx().getExecutor().createQuerySession();
            if (!session.isSuccess()) {
                onComplete(session.getStatus(), null);
            }

            logger.debug("execute YQL [{}]", yql);

            ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                    .withGrpcFlowControl(flowControl)
                    .build();
            stream = session.getValue().createQuery(yql, TxMode.NONE, Params.empty(), settings);
            stream.execute(part -> onNextPart(part.getResultSetReader())).whenComplete((res, th) -> {
                session.getValue().close();
                onComplete(res.getStatus(), th);
            });

            return yql;
        }

        @Override
        protected void cancel() {
            if (stream != null) {
                stream.cancel();
            }
        }
    }
}
