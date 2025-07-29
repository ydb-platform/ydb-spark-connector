package tech.ydb.spark.connector.read;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.KeysRange;
import tech.ydb.table.Session;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.TupleValue;

/**
 * Scan is the factory for the Batch.
 *
 * @author zinal
 */
public class YdbReadTable implements Batch, Scan, ScanBuilder, SupportsReportPartitioning, PartitionReaderFactory,
        SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit {
    private static final Logger logger = LoggerFactory.getLogger(YdbReadTable.class);
    private static final long serialVersionUID = -5790675592880793417L;

    private final YdbTable table;
    private final YdbTypes types;
    private final int queueMaxSize;
    private final FieldInfo[] keys;

    private int rowLimit;
    private KeysRange predicateRange;
    private StructType readSchema;

    public YdbReadTable(YdbTable table, CaseInsensitiveStringMap options) {
        this.table = table;

        this.types = new YdbTypes(options);
        this.queueMaxSize = StreamReader.readQueueMaxSize(options);
        this.keys = table.getKeyColumns();

        this.predicateRange = KeysRange.UNRESTRICTED;

        this.rowLimit = -1;
        this.readSchema = table.schema();
    }

    @Override
    public StructType readSchema() {
        return readSchema;
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
    public Predicate[] pushPredicates(Predicate[] predicates) {
        if (predicates == null || predicates.length == 0) {
            return predicates;
        }
        List<Predicate> flat = flattenPredicates(predicates);
        detectRangeSimple(flat);
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
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        ShardPartition p = (ShardPartition) partition;
        return new ReadTableReader(p.getRange());
    }

    @Override
    public Partitioning outputPartitioning() {
        KeysRange[] partitions = table.getPartitions();

        // TODO: KeyGroupedPartitioning (requires HasPartitionKey for partitions)
        if (partitions.length == 0) {
            return new UnknownPartitioning(1);
        }
        logger.info("Output {} unknown partitions", partitions.length);
        return new UnknownPartitioning(partitions.length);
    }

    @Override
    public InputPartition[] planInputPartitions() {
        KeysRange[] partitions = table.getPartitions();
        if (partitions.length == 0) {
            logger.warn("Missing partitioning information for table {}", table.getTablePath());
            // Single partition with possible limits taken from the predicates.
            return new InputPartition[] {new ShardPartition(0, KeysRange.UNRESTRICTED)};
        }
        logger.debug("Input table partitions: {}", Arrays.toString(partitions));
        final Random random = new Random();
        ShardPartition[] out = Stream.of(partitions)
                .map(kr -> kr.intersect(predicateRange))
                .filter(kr -> !kr.isEmpty())
                .map(kr -> new ShardPartition(random.nextInt(999999999), kr))
                .toArray(ShardPartition[]::new);

        logger.debug("Input partitions count {}, filtered partitions count {}", partitions.length, out.length);
        logger.debug("Filtered partition ranges: {}", Arrays.toString(out));
        // Random ordering is better for multiple  concurrent scans with limited parallelism.
        Arrays.sort(out, (p1, p2) -> Integer.compare(p1.getOrderingKey(), p2.getOrderingKey()));
        return out;
    }

    /**
     * Put all predicates connected with AND directly into the list of predicates, recursively.
     *
     * @param filters Input filters
     * @return Flattened predicates
     */
    private List<Predicate> flattenPredicates(Predicate[] predicates) {
        final List<Predicate> retval = new ArrayList<>();
        for (Predicate p : predicates) {
            flattenPredicate(p, retval);
        }
        return retval;
    }

    /**
     * Put all filters connected with AND directly into the list of filters, recursively.
     *
     * @param f Input filter to be processed
     * @param retval The resulting list of flattened filters
     */
    private void flattenPredicate(Predicate p, List<Predicate> retval) {
        if ("AND".equalsIgnoreCase(p.name())) {
            And fand = (And) p;
            flattenPredicate(fand.left(), retval);
            flattenPredicate(fand.right(), retval);
        } else {
            retval.add(p);
        }
    }

    /**
     * Very basic filter-to-range conversion logic. Currently covers N equality conditions + 1
     * optional following range condition. Does NOT handle complex cases like N-dimensional ranges.
     *
     * @param predicates input list of filters
     */
    private void detectRangeSimple(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return;
        }
        logger.debug("Calculating scan ranges for predicates {}", predicates);
        Serializable[] rangeBegin = new Serializable[keys.length];
        Serializable[] rangeEnd = new Serializable[keys.length];

        for (int pos = 0; pos < keys.length; ++pos) {
            final String keyColumn = keys[pos].getName();
            boolean hasEquality = false;
            for (Predicate p : predicates) {
                final String pname = p.name();
                if ("=".equalsIgnoreCase(pname) || "<=>".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        rangeBegin[pos] = lyzer.value;
                        rangeEnd[pos] = lyzer.value;
                        hasEquality = true;
                        break; // we got both upper and lower bounds, moving to next key column
                    }
                } else if (">".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeEnd[pos] = YdbTypes.min(rangeEnd[pos], lyzer.value);
                        } else {
                            rangeBegin[pos] = YdbTypes.max(rangeBegin[pos], lyzer.value);
                        }
                    }
                } else if (">=".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeEnd[pos] = YdbTypes.min(rangeEnd[pos], lyzer.value);
                        } else {
                            rangeBegin[pos] = YdbTypes.max(rangeBegin[pos], lyzer.value);
                        }
                    }
                } else if ("<".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeBegin[pos] = YdbTypes.max(rangeBegin[pos], lyzer.value);
                        } else {
                            rangeEnd[pos] = YdbTypes.min(rangeEnd[pos], lyzer.value);
                        }
                    }
                } else if ("<=".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeBegin[pos] = YdbTypes.max(rangeBegin[pos], lyzer.value);
                        } else {
                            rangeEnd[pos] = YdbTypes.min(rangeEnd[pos], lyzer.value);
                        }
                    }
                } else if ("STARTS_WITH".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success && !lyzer.revert) {
                        String lvalue = lyzer.value.toString();
                        if (lvalue.length() > 0) {
                            int lastCharPos = lvalue.length() - 1;
                            String rvalue = new StringBuilder()
                                    .append(lvalue, 0, lastCharPos)
                                    .append((char) (1 + lvalue.charAt(lastCharPos)))
                                    .toString();
                            rangeBegin[pos] = YdbTypes.max(rangeBegin[pos], lvalue);
                            rangeEnd[pos] = YdbTypes.min(rangeEnd[pos], rvalue);
                        }
                    }
                }
            } // for (Predicate p : ...)
            if (!hasEquality) {
                break;
            }
        }

        predicateRange = new KeysRange(rangeBegin, true, rangeEnd, true);
        logger.debug("Calculated scan ranges {}", predicateRange);
    }

    /**
     * Too small to be called "Analyzer"
     */
    static final class Lyzer {

        final boolean success;
        final boolean revert;
        final Serializable value;

        Lyzer(String keyColumn, Expression[] children) {
            boolean localSuccess = false;
            boolean localRevert = false;
            Serializable localValue = null;
            if (children.length == 2) {
                Expression left = children[0];
                Expression right = children[1];
                if (right instanceof FieldReference) {
                    Expression temp = right;
                    right = left;
                    left = temp;
                    localRevert = true;
                }
                if (left instanceof FieldReference
                        && left.references().length > 0
                        && right instanceof LiteralValue) {
                    NamedReference nr = left.references()[left.references().length - 1];
                    if (nr.fieldNames().length > 0) {
                        String fieldName = nr.fieldNames()[nr.fieldNames().length - 1];
                        if (keyColumn.equals(fieldName)) {
                            LiteralValue<?> lv = (LiteralValue<?>) right;
                            localValue = (Serializable) lv.value();
                            localSuccess = true;
                        }
                    }
                }
            }
            this.success = localSuccess;
            this.revert = localRevert;
            this.value = localValue;
        }
    }

    private final class ReadTableReader extends StreamReader {
        private final String id;
        private final String tablePath;
        private final ReadTableSettings settings;
        private volatile GrpcReadStream<ReadTablePart> stream;

        ReadTableReader(KeysRange keysRange) {
            super(types, queueMaxSize, readSchema);

            this.tablePath = table.getTablePath();

            List<String> columnsToRead = new ArrayList<>(Arrays.asList(readSchema.fieldNames()));
            if (columnsToRead.isEmpty()) {
                columnsToRead.add(table.getKeyColumns()[0].getName());
            }

            ReadTableSettings.Builder rtsb = ReadTableSettings.newBuilder()
                    // TODO: add setting for the maximum scan duration.
                    .withRequestTimeout(Duration.ofHours(8))
                    .withGrpcFlowControl(flowControl)
                    .orderedRead(true)
                    .columns(columnsToRead);

            if (keysRange.hasFromValue()) {
                TupleValue tv = keysRange.readFromValue(types, table.getKeyColumns());
                if (keysRange.includesFromValue()) {
                    rtsb.fromKeyInclusive(tv);
                } else {
                    rtsb.fromKeyExclusive(tv);
                }
            }
            if (keysRange.hasToValue()) {
                TupleValue tv = keysRange.readToValue(types, table.getKeyColumns());
                if (keysRange.includesToValue()) {
                    rtsb.toKeyInclusive(tv);
                } else {
                    rtsb.toKeyExclusive(tv);
                }
            }

            if (rowLimit > 0) {
                rtsb.rowLimit(rowLimit);
            }

            String columns = columnsToRead.stream().collect(Collectors.joining(","));
            this.id = "READ TABLE " + columns + " RANGE " + keysRange + " LIMIT " + rowLimit;
            this.settings = rtsb.build();
        }

        @Override
        protected String start() {
            // Execute read table
            Result<Session> session = table.getCtx().getExecutor().createTableSession();
            if (!session.isSuccess()) {
                onComplete(session.getStatus(), null);
            }

            this.stream = session.getValue().executeReadTable(tablePath, settings);
            stream.start(part -> onNextPart(part.getResultSetReader())).whenComplete((status, th) -> {
                session.getValue().close();
                onComplete(status, th);
            });

            return id;
        }

        @Override
        protected void cancel() {
            if (stream != null) {
                stream.cancel();
            }
        }
    }
}
