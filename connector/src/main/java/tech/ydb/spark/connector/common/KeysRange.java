package tech.ydb.spark.connector.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.sparkproject.guava.annotations.VisibleForTesting;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.description.KeyBound;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * Key range: two border vectors, plus the border inclusivity flag.
 *
 * @author zinal
 */
public class KeysRange implements Serializable {
    private static final long serialVersionUID = -952838545708943414L;

    public static final KeysRange UNRESTRICTED = new KeysRange(Limit.UNSTRICTED, Limit.UNSTRICTED);
    public static final KeysRange EMPTY = new KeysRange((Limit) null, (Limit) null);

    private final Limit from;
    private final Limit to;

    public KeysRange(KeyRange kr, YdbTypes types) {
        this(new Limit(kr.getFrom(), types), new Limit(kr.getTo(), types));
    }

    public KeysRange(Serializable[] fromValue, boolean fromInclusive, Serializable[] toValue, boolean toInclusive) {
        this(new Limit(fromValue, fromInclusive), new Limit(toValue, toInclusive));
    }

    private KeysRange(Limit left, Limit right) {
        if (isValidRange(left, right)) {
            this.from = left;
            this.to = right;
        } else {
            this.from = null;
            this.to = null;
        }
    }

    public boolean isEmpty() {
        return from == null || to == null;
    }

    public boolean hasFromValue() {
        return from != null && from.values != null;
    }

    public boolean hasToValue() {
        return to != null && to.values != null;
    }

    public boolean isUnrestricted() {
        return from != null && to != null && from.values == null && to.values == null;
    }

    public boolean includesFromValue() {
        return from != null && from.inclusive;
    }

    public boolean includesToValue() {
        return to != null && to.inclusive;
    }

    public TupleValue readFromValue(YdbTypes types, FieldInfo[] columns) {
        return from.writeTuple(types, columns);
    }

    public TupleValue readToValue(YdbTypes types, FieldInfo[] columns) {
        return to.writeTuple(types, columns);
    }

    public KeysRange intersect(KeysRange other) {
        if (isEmpty() || other == null || other.isUnrestricted()) {
            return this;
        }
        if (other.isEmpty() || isUnrestricted()) {
            return other;
        }
        return new KeysRange(
                from.leftMerge(other.from),
                to.rightMerge(other.to)
        );
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "(None)";
        }
        char l = from.inclusive ? '[' : '(';
        char r = to.inclusive ? ']' : ')';
        return l + toString(from.values, "-Inf") + " - " + toString(to.values, "+Inf") + r;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KeysRange other = (KeysRange) obj;
        return Objects.equals(this.to, other.to) && Objects.equals(this.from, other.from);
    }

    private static String toString(Serializable[] value, String nullValue) {
        if (value == null || value.length == 0) {
            return nullValue;
        }
        if (value.length == 1) {
            return value[0].toString();
        }

        String[] ss = new String[value.length];
        for (int idx = 0; idx < value.length; idx += 1) {
            ss[idx] = value[idx].toString();
        }

        return "(" + String.join(",", ss) + ")";
    }

    @VisibleForTesting
    static int compareValues(Serializable[] v1, Serializable[] v2) {
        if (v1 == v2) {
            return 0; // the same values
        }

        if (v1 == null) {
            return -1;
        }

        if (v2 == null) {
            return 1;
        }

        for (int idx = 0; idx < v1.length && idx < v2.length; idx += 1) {
            Serializable o1 = v1[idx];
            Serializable o2 = v2[idx];
            if (o1 == null) {
                return o2 == null ? 0 : -1;
            }

            if (o2 == null) {
                return 1;
            }

            if (o1 == o2) {
                continue;
            }
            Class<?> type = o1.getClass();
            if (type != o2.getClass()) {
                throw new IllegalArgumentException("Incompatible data types " + type + " and " + o2.getClass());
            }
            if (!Comparable.class.isAssignableFrom(type)) {
                throw new IllegalArgumentException("Uncomparable data type " + type);
            }

            @SuppressWarnings("unchecked")
            final int cmp = ((Comparable) o1).compareTo(o2);
            if (cmp != 0) {
                return cmp;
            }
        }

        if (v1.length < v2.length && v2[v1.length] != null) {
            return -1;
        }

        if (v2.length < v1.length && v1[v2.length] != null) {
            return 1;
        }

        return 0;
    }

    private static boolean isValidRange(Limit from, Limit to) {
        // empty range
        if (from == null || to == null) {
            return false;
        }
        // range with one or both unrestricted bounds is always correct
        if (from.values == null || to.values == null) {
            return true;
        }

        int cmp = compareValues(from.values, to.values);
        if (cmp < 0) { // from < to is always a valid range
            return true;
        }
        if (cmp > 0) { // from > to is always an invalid range
            return false;
        }
        // range for a single value is valid only when both bounds are inclusive
        return from.inclusive && to.inclusive;
    }

    public static Serializable[] readTuple(Optional<KeyBound> key, YdbTypes types) {
        if (!key.isPresent()) {
            return null;
        }

        Value<?> value = key.get().getValue();
        if (!(value instanceof TupleValue)) {
            throw new IllegalArgumentException();
        }

        TupleValue tv = (TupleValue) value;
        int sz = tv.size();
        Serializable[] out = new Serializable[sz];

        for (int i = 0; i < sz; ++i) {
            out[i] = types.ydb2pojo(tv.get(i));

            if (out[i] == null) { // can reduce tuple until first null
                Serializable[] reduced = new Serializable[i];
                System.arraycopy(out, 0, reduced, 0, i);
                return reduced;
            }
        }
        return out;
    }

    public static Serializable[] validateTuple(Serializable[] value) {
        if (value == null || value.length == 0 || value[0] == null) {
            return null;
        }

        int len = 0;
        while (len < value.length && value[len] != null) {
            len += 1;
        }

        if (len == value.length) {
            return value;
        }

        Serializable[] validated = new Serializable[len];
        System.arraycopy(value, 0, validated, 0, len);
        return validated;
    }

    private static class Limit implements Serializable {
        private static final long serialVersionUID = -2062171265432646099L;
        private static final Limit UNSTRICTED = new Limit(Optional.empty(), null);

        private final Serializable[] values;
        private final boolean inclusive;

        Limit(Serializable[] values, boolean inclusive) {
            this.values = validateTuple(values);
            // inf cannot be inclusive
            this.inclusive = this.values != null && inclusive;
        }

        Limit(Optional<KeyBound> key, YdbTypes types) {
            this.values = readTuple(key, types);
            this.inclusive = key.isPresent() && key.get().isInclusive();
        }

        @Override
        public int hashCode() {
            return Objects.hash(inclusive, Arrays.hashCode(values));
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Limit other = (Limit) obj;
            return this.inclusive == other.inclusive && Arrays.equals(this.values, other.values);
        }

        Limit leftMerge(Limit other) {
            if (values == null) {
                return other;
            }
            if (other.values == null) {
                return this;
            }

            int cmp = compareValues(values, other.values);
            if (cmp < 0) {
                return other;
            }
            if (cmp > 0) {
                return this;
            }

            return new Limit(values, inclusive && other.inclusive);
        }

        Limit rightMerge(Limit other) {
            if (values == null) {
                return other;
            }
            if (other.values == null) {
                return this;
            }

            int cmp = compareValues(values, other.values);
            if (cmp < 0) {
                return this;
            }
            if (cmp > 0) {
                return other;
            }

            return new Limit(values, inclusive && other.inclusive);
        }

        TupleValue writeTuple(YdbTypes types, FieldInfo[] columns) {
            final List<Value<?>> l = new ArrayList<>(values.length);
            for (int i = 0; i < values.length; ++i) {
                Value<?> v = types.convertToYdb(values[i], columns[i].getType());
                if (!v.getType().getKind().equals(Type.Kind.OPTIONAL)) {
                    v = v.makeOptional();
                }
                l.add(v);
            }
            return TupleValue.of(l);
        }
    }
}
