package tech.ydb.spark.connector.read;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;

/**
 * Scan is the factory for the Batch.
 *
 * @author zinal
 */
public class YdbScanTable implements Batch, Scan, ScanBuilder, SupportsReportPartitioning, PartitionReaderFactory,
        SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit  {
    private static final long serialVersionUID = 6752417702512593851L;

    private final YdbTable table;
    private final YdbTypes types;
    private final int queueMaxSize;

    private int rowLimit;
    private StructType readSchema;

    public YdbScanTable(YdbTable table, CaseInsensitiveStringMap options) {
        this.table = table;
        this.types = new YdbTypes(options);

        this.queueMaxSize = LazyReader.readQueueMaxSize(options);
        this.rowLimit = -1;
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
        TabletPartition p = (TabletPartition) partition;
        return new QueryServiceReader(p.getTabletId());
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        return predicates; // all predicates should be re-checked
    }

    @Override
    public Predicate[] pushedPredicates() {
        return new Predicate[]{}; // all predicates should be re-checked
    }

    @Override
    public boolean pushLimit(int limit) {
        this.rowLimit = limit;
        return false;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.readSchema = requiredSchema;
    }

    @Override
    public Partitioning outputPartitioning() {
        List<String> tablets = table.getCtx().getExecutor().getTabletIds(table.getTablePath());
        if (tablets.isEmpty()) {
            return new UnknownPartitioning(1);
        }
        return new UnknownPartitioning(tablets.size());
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<String> tablets = table.getCtx().getExecutor().getTabletIds(table.getTablePath());
        if (tablets.isEmpty()) {
            return new InputPartition[]{new TabletPartition(null)};
        }
        InputPartition[] partitions = new InputPartition[tablets.size()];
        int idx = 0;
        for (String id: tablets) {
            partitions[idx++] = new TabletPartition(id);
        }
        return partitions;
    }

    private final class QueryServiceReader extends LazyReader {
        private final String query;
        private volatile QueryStream stream = null;

        QueryServiceReader(String tabletId) {
            super(types, queueMaxSize, readSchema);

            List<String> columns = new ArrayList<>(Arrays.asList(readSchema.fieldNames()));
            if (columns.isEmpty()) {
                columns.add(table.getKeyColumns()[0].getName());
            }

            StringBuilder sb = new StringBuilder("SELECT");
            char dep = ' ';
            for (String name: columns) {
                sb.append(dep);
                sb.append('`');
                sb.append(name);
                sb.append('`');
                dep = ',';
            }
            sb.append(" FROM `").append(table.getTablePath()).append("`");

            if (tabletId != null) {
                sb.append(" WITH TabletId='").append(tabletId).append("'");
            }

            if (rowLimit > 0) {
                sb.append(" LIMIT ").append(rowLimit);
            }

            this.query = sb.toString();
        }

        @Override
        protected String start() {
            Result<QuerySession> session = table.getCtx().getExecutor().createQuerySession();
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
}
