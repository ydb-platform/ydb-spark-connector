package tech.ydb.spark.connector.read;


import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.KeysRange;
import tech.ydb.table.query.Params;

/**
 * Scan is the factory for the Batch.
 *
 * @author zinal
 */
public class YdbScanTable implements Batch, Scan, ScanBuilder, SupportsReportPartitioning, PartitionReaderFactory,
        SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit, SupportsPushDownAggregates  {
    private static final long serialVersionUID = 6752417702512593851L;
    private static final Logger logger = LoggerFactory.getLogger(YdbScanTable.class);

    private final YdbTable table;
    private final SelectQuery query;
    private final YdbTypes types;
    private final int queueMaxSize;

    private StructType readSchema;

    public YdbScanTable(YdbTable table, CaseInsensitiveStringMap options) {
        this.table = table;
        this.query = new SelectQuery(table);
        this.types = new YdbTypes(options);

        this.queueMaxSize = LazyReader.readQueueMaxSize(options);
        this.readSchema = table.schema();
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public Scan build() {
        return this;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return this;
    }

    @Override
    public StructType readSchema() {
        return readSchema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        YdbPartition p = (YdbPartition) partition;
        return new QueryServiceReader(p.makeQuery(query));
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        logger.debug("push predicates {}", Arrays.toString(predicates));
        return predicates; // all predicates should be re-checked
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
        logger.debug("request complete aggregation {} with group by {}",
                Arrays.toString(aggregation.aggregateExpressions()),
                Arrays.toString(aggregation.groupByExpressions())
        );
        return false;
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        logger.debug("push aggregation {} with group by {}",
                Arrays.toString(aggregation.aggregateExpressions()),
                Arrays.toString(aggregation.groupByExpressions())
        );
        return false;
    }

    @Override
    public Predicate[] pushedPredicates() {
        return new Predicate[]{}; // all predicates should be re-checked
    }

    @Override
    public boolean pushLimit(int limit) {
        logger.debug("push limit {}", limit);
        query.withRowLimit(limit);
        return false; // limit should be re-applied
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        logger.debug("prune columns {}", Arrays.toString(requiredSchema.names()));
        this.readSchema = requiredSchema;
        if (requiredSchema.length() > 0) {
            query.replacePredicates(requiredSchema.names());
        }
    }

    @Override
    public Partitioning outputPartitioning() {
        switch (table.getType()) {
            case COLUMN:
                List<String> tablets = table.getCtx().getExecutor().getTabletIds(table.getTablePath());
                if (!tablets.isEmpty()) {
                    return new UnknownPartitioning(tablets.size());
                }
                break;
            case ROW:
            case INDEX:
            default:
                KeysRange[] partitions = table.getPartitions();
                if (partitions.length != 0) {
                    return new UnknownPartitioning(partitions.length);
                }
                break;
        }
        return new UnknownPartitioning(1);
    }

    private static <T> T[] shuffle(T[] array) {
        Random rnd = new Random();
        int count = array.length;
        for (int i = count; i > 1; i--) {
            int r = rnd.nextInt(i);
            T temp = array[i - 1];
            array[i - 1] = array[r];
            array[r] = temp;
        }
        return array;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        switch (table.getType()) {
            case COLUMN:
                List<String> tablets = table.getCtx().getExecutor().getTabletIds(table.getTablePath());
                if (!tablets.isEmpty()) {
                    InputPartition[] partitions = new InputPartition[tablets.size()];
                    int idx = 0;
                    for (String id: tablets) {
                        partitions[idx++] = YdbPartitions.tabletId(id);
                    }
                    return shuffle(partitions);
                }
                break;
            case ROW:
            case INDEX:
            default:
                KeysRange[] ranges = table.getPartitions();
                if (ranges.length > 0) {
                    InputPartition[] partitions = new InputPartition[ranges.length];
                    for (int idx = 0; idx < ranges.length; idx++) {
                        partitions[idx] = YdbPartitions.keysRange(types, table.getKeyColumns(), ranges[idx]);
                    }
                    return shuffle(partitions);
                }
                break;
        }

        return new InputPartition[]{YdbPartitions.none()};
    }

    private final class QueryServiceReader extends LazyReader {
        private final String query;
        private final Params params;
        private volatile QueryStream stream = null;

        QueryServiceReader(SelectQuery query) {
            super(types, queueMaxSize, readSchema);
            this.query = query.toQuery();
            this.params = query.toQueryParams();
        }

        @Override
        protected String start() {
            Result<QuerySession> session = table.getCtx().getExecutor().createQuerySession();
            if (!session.isSuccess()) {
                onComplete(session.getStatus(), null);
            }

            stream = session.getValue().createQuery(query, TxMode.SNAPSHOT_RO, params);
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
}
