package tech.ydb.spark.connector.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.description.KeyBound;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;

/**
 * Key range: two border vectors, plus the border inclusivity flag.
 *
 * @author zinal
 */
public class KeysRange implements Serializable {
    private static final long serialVersionUID = 5756661733369903758L;

    public static final Limit NO_LIMIT = new Limit(new Serializable[0], true);
    public static final KeysRange UNRESTRICTED = new KeysRange(NO_LIMIT, NO_LIMIT);

    private final Limit from;
    private final Limit to;

    public KeysRange(Limit from, Limit to) {
        this.from = validated(from);
        this.to = validated(to);
    }

    public KeysRange(KeyRange kr, YdbTypes types) {
        this(convert(kr.getFrom(), types), convert(kr.getTo(), types));
    }

//    public KeysRange(ArrayList<Object> from, ArrayList<Object> to) {
//        this(new Limit(from, true), new Limit(to, false));
//    }

//    public KeysRange(List<Object> from, List<Object> to) {
//        this(new Limit(from, true), new Limit(to, false));
//    }

    public KeysRange(Serializable[] from, boolean fromInclusive, Serializable[] to, boolean toInclusive) {
        this(new Limit(from, fromInclusive), new Limit(to, toInclusive));
    }

    public Limit getFrom() {
        return from;
    }

    public Limit getTo() {
        return to;
    }

    /**
     * Empty range means left is greater than right. Missing values on left means MIN, on right -
     * MAX.
     *
     * @return true for empty range, false otherwise
     */
    public boolean isEmpty() {
        for (int idx = 0; idx < from.values.length && idx < to.values.length; idx += 1) {
            final Serializable o1 = from.values[idx];
            final Serializable o2 = to.values[idx];
            if (o1 == o2) {
                continue;
            }
            if (o1 == null) {
                return false;
            }
            if (o2 == null) {
                return false;
            }
            if (o1.getClass() != o2.getClass()) {
                throw new IllegalArgumentException("Incompatible data types " + o1.getClass().toString()
                        + " and " + o2.getClass().toString());
            }
            if (!(o1 instanceof Comparable)) {
                throw new IllegalArgumentException("Uncomparable data type " + o1.getClass().toString());
            }
            @SuppressWarnings("unchecked")
            int cmp = ((Comparable) o1).compareTo(o2);
            if (cmp > 0) {
                return true;
            }
            if (cmp < 0) {
                return false;
            }
        }
        if (!from.inclusive) {
            if ((from.values.length < to.values.length) || to.inclusive) {
                return true;
            }
        }
        if (!to.inclusive) {
            if ((from.values.length > to.values.length) || from.inclusive) {
                return true;
            }
        }
        return false;
    }

    public boolean isUnrestricted() {
        return from.isUnrestricted() && to.isUnrestricted();
    }

    public static Limit convert(Optional<KeyBound> v, YdbTypes types) {
        if (!v.isPresent()) {
            return null;
        }

        KeyBound kb = v.get();
        Value<?> tx = kb.getValue();
        if (!(tx instanceof TupleValue)) {
            throw new IllegalArgumentException();
        }
        TupleValue t = (TupleValue) tx;
        final int sz = t.size();
        Serializable[] out = new Serializable[sz];
        for (int i = 0; i < sz; ++i) {
            out[i] = types.convertFromYdb(t.get(i));
        }
        return new Limit(out, kb.isInclusive());
    }

    @Override
    public String toString() {
        return (from.isInclusive() ? "[* " : "(* ")
                + Arrays.toString(from.values) + " -> " + Arrays.toString(to.values)
                + (to.isInclusive() ? " *]" : " *)");
    }

    public KeysRange intersect(KeysRange other) {
        if (other == null || (other.from.isUnrestricted() && other.to.isUnrestricted())) {
            return this;
        }
        if (from.isUnrestricted() && to.isUnrestricted()) {
            return other;
        }
        Limit outFrom = (from.compareTo(other.from, true) > 0) ? from : other.from;
        Limit outTo = (to.compareTo(other.to, false) > 0) ? other.to : to;
        KeysRange retval = new KeysRange(outFrom, outTo);
        return retval;
    }

    public static int compare(Serializable[] v1, Serializable[] v2, boolean nullsFirst) {
        if (v1 == v2 || (v1 != null && v2 != null && v1.length == 0 && v2.length == 0)) {
            return 0;
        }
        if (v1 == null || v1.length == 0) {
            return nullsFirst ? -1 : 1;
        }
        if (v2 == null || v2.length == 0) {
            return nullsFirst ? 1 : -1;
        }

        for (int idx = 0; idx < v1.length && idx < v2.length; idx += 1) {
            Serializable o1 = v1[idx];
            Serializable o2 = v2[idx];
            if (o1 == o2) {
                continue;
            }
            if (o1 == null) {
                return nullsFirst ? -1 : 1;
            }
            if (o2 == null) {
                return nullsFirst ? 1 : -1;
            }
            if (o1.getClass() != o2.getClass()) {
                throw new IllegalArgumentException("Incompatible data types "
                        + o1.getClass().toString() + " and " + o2.getClass().toString());
            }
            if (!(o1 instanceof Comparable)) {
                throw new IllegalArgumentException("Uncomparable data type "
                        + o1.getClass().toString());
            }
            @SuppressWarnings("unchecked")
            final int cmp = ((Comparable) o1).compareTo(o2);
            if (cmp != 0) {
                return cmp;
            }
        }
        if (v1.length < v2.length) {
            return nullsFirst ? -1 : 1;
        }
        if (v1.length > v2.length) {
            return nullsFirst ? 1 : -1;
        }

        return 0;
    }

    private static Limit validated(Limit v) {
        if (v == null) {
            return NO_LIMIT;
        }
        if (v.isUnrestricted()) {
            return NO_LIMIT;
        }
        int pos = v.values.length;
        while (pos > 0) {
            Object o = v.values[pos - 1];
            if (o != null) {
                break;
            }
            pos -= 1;
        }
        if (pos == v.values.length) {
            return v;
        }
        if (pos < 1) {
            return NO_LIMIT;
        }
        Serializable[] cleaned = new Serializable[pos];
        System.arraycopy(v.values, 0, cleaned, 0, pos);
        return new Limit(cleaned, v.inclusive);
    }

    public static class Limit implements Serializable {
        private static final long serialVersionUID = -3278687786440323269L;

        private final Serializable[] values;
        private final boolean inclusive;

        public Limit(Serializable[] values, boolean inclusive) {
            this.values = values;
            this.inclusive = inclusive;
        }

        public Serializable[] getValues() {
            return values;
        }

        public boolean isInclusive() {
            return inclusive;
        }

        public boolean isUnrestricted() {
            return values == null || values.length == 0;
        }

        @Override
        public int hashCode() {
            return 2 * Arrays.hashCode(values) + (this.inclusive ? 1 : 0);
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
            final Limit other = (Limit) obj;
            if (this.inclusive != other.inclusive) {
                return false;
            }
            return Arrays.equals(this.values, other.values);
        }

        @Override
        public String toString() {
            return "{" + "value=" + Arrays.toString(values) + ", inclusive=" + inclusive + '}';
        }

        public int compareTo(Limit t, boolean nullsFirst) {
            int cmp = compare(this.values, t.values, nullsFirst);
            if (cmp == 0) {
                if (this.inclusive == t.inclusive) {
                    return 0;
                }
                if (this.inclusive) {
                    return nullsFirst ? -1 : 1;
                }
                return nullsFirst ? 1 : -1;
            }
            return cmp;
        }
    }

}
