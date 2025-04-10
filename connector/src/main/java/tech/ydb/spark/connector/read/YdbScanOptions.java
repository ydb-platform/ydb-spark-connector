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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.KeysRange;
import tech.ydb.spark.connector.common.OperationOption;

/**
 * All settings for the scan operations, shared between the partition readers.
 *
 * @author zinal
 */
public class YdbScanOptions implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(YdbScanOptions.class);
    private static final long serialVersionUID = -4608401766953066647L;

    private final YdbTypes types;
    private final FieldInfo[] keys;
    private final int scanQueueDepth;

    private int rowLimit;
    private KeysRange predicateRange;
    private StructType readSchema;

    public YdbScanOptions(YdbTable table, CaseInsensitiveStringMap options) {
        this.types = new YdbTypes(options);
        this.keys = table.getKeyColumns();

        this.predicateRange = KeysRange.UNRESTRICTED;

        this.scanQueueDepth = getScanQueueDepth(options);
        this.rowLimit = -1;
        this.readSchema = table.schema();
    }

    public YdbTypes getTypes() {
        return types;
    }

    public int getScanQueueDepth() {
        return scanQueueDepth;
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

    public void pruneColumns(StructType requiredSchema) {
        this.readSchema = requiredSchema;
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

    public void setupPredicates(Predicate[] predicates) {
        if (predicates == null || predicates.length == 0) {
            return;
        }
        List<Predicate> flat = flattenPredicates(predicates);
        detectRangeSimple(flat);
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

    private static int getScanQueueDepth(CaseInsensitiveStringMap options) {
        try {
            int scanQueueDepth = OperationOption.SCAN_QUEUE_DEPTH.readInt(options, 3);
            if (scanQueueDepth < 2) {
                logger.warn("Value of {} property too low, reverting to minimum of 2.",
                        OperationOption.SCAN_QUEUE_DEPTH);
                return 2;
            }

            return scanQueueDepth;
        } catch (NumberFormatException nfe) {
            logger.warn("Illegal value of {} property, reverting to default of 3.",
                    OperationOption.SCAN_QUEUE_DEPTH, nfe);
            return 3;
        }
    }
}
