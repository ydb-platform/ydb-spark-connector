package tech.ydb.spark.connector.read;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.KeysRange;

/**
 * All settings for the scan operations, shared between the partition readers.
 *
 * @author zinal
 */
public class YdbReadTableOptions implements Serializable,
        ScanBuilder, SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit {
    private static final Logger logger = LoggerFactory.getLogger(YdbReadTableOptions.class);
    private static final long serialVersionUID = -5865188993174204708L;

    private final YdbTable table;
    private final YdbTypes types;
    private final int queueMaxSize;
    private final FieldInfo[] keys;

    private int rowLimit;
    private KeysRange predicateRange;
    private StructType readSchema;

    public YdbReadTableOptions(YdbTable table, CaseInsensitiveStringMap options) {
        this.table = table;
        this.types = new YdbTypes(options);
        this.queueMaxSize = LazyReader.readQueueMaxSize(options);
        this.keys = table.getKeyColumns();

        this.predicateRange = KeysRange.UNRESTRICTED;

        this.rowLimit = -1;
        this.readSchema = table.schema();
    }

    @Override
    public Scan build() {
        return new YdbReadTable(table, this);
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

    public YdbTypes getTypes() {
        return types;
    }

    public int getMaxQueueSize() {
        return queueMaxSize;
    }

    public StructType getReadSchema() {
        return readSchema;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public KeysRange getPredicateRange() {
        return predicateRange;
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
}
