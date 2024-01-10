package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import tech.ydb.table.description.KeyBound;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;

/**
 * Key range: two border vectors, plus the border inclusivity flag.
 *
 * @author zinal
 */
public class YdbKeyRange implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Limit NO_LIMIT = new Limit(new ArrayList<>(), true);
    public static final YdbKeyRange UNRESTRICTED = new YdbKeyRange(NO_LIMIT, NO_LIMIT);

    private final Limit from;
    private final Limit to;

    public YdbKeyRange(Limit from, Limit to) {
        this.from = (from == null) ? NO_LIMIT : cleanup(from);
        this.to = (to == null) ? NO_LIMIT : cleanup(to);
    }

    public YdbKeyRange(KeyRange kr, YdbTypes types) {
        this(convert(kr.getFrom(), types), convert(kr.getTo(), types));
    }

    public YdbKeyRange(List<Object> from, List<Object> to) {
        this(new Limit(from, true), new Limit(to, false));
    }

    public YdbKeyRange(Object[] from, Object[] to) {
        this(new Limit(Arrays.asList(from), true), new Limit(Arrays.asList(to), false));
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
        final Iterator<?> i1 = from.value.iterator();
        final Iterator<?> i2 = to.value.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            final Object o1 = i1.next();
            final Object o2 = i2.next();
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
                throw new IllegalArgumentException("Incompatible data types "
                        + o1.getClass().toString() + " and " + o2.getClass().toString());
            }
            if (!(o1 instanceof Comparable)) {
                throw new IllegalArgumentException("Uncomparable data type "
                        + o1.getClass().toString());
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
            if (!i1.hasNext()) {
                return true;
            }
        }
        if (!to.inclusive) {
            if (!i2.hasNext()) {
                return true;
            }
        }
        return false;
    }

    public boolean isUnrestricted() {
        return from.isUnrestricted() && to.isUnrestricted();
    }

    public static Limit convert(Optional<KeyBound> v, YdbTypes types) {
        if (v.isPresent()) {
            KeyBound kb = v.get();
            Value<?> tx = kb.getValue();
            if (!(tx instanceof TupleValue)) {
                throw new IllegalArgumentException();
            }
            TupleValue t = (TupleValue) tx;
            final int sz = t.size();
            List<Object> out = new ArrayList<>(sz);
            for (int i = 0; i < sz; ++i) {
                out.add(types.convertFromYdb(t.get(i)));
            }
            return new Limit(out, kb.isInclusive());
        }
        return null;
    }

    @Override
    public String toString() {
        return (from.isInclusive() ? "[* " : "(* ")
                + from.value + " -> " + to.value
                + (to.isInclusive() ? " *]" : " *)");
    }

    public YdbKeyRange intersect(YdbKeyRange other) {
        if (other == null
                || (other.from.isUnrestricted() && other.to.isUnrestricted())) {
            return this;
        }
        if (from.isUnrestricted() && to.isUnrestricted()) {
            return other;
        }
        Limit outFrom = (from.compareTo(other.from, true) > 0) ? from : other.from;
        Limit outTo = (to.compareTo(other.to, false) > 0) ? other.to : to;
        YdbKeyRange retval = new YdbKeyRange(outFrom, outTo);
        return retval;
    }

    public static int compare(List<Object> v1, List<Object> v2, boolean nullsFirst) {
        if (v1 == null) {
            v1 = Collections.emptyList();
        }
        if (v2 == null) {
            v2 = Collections.emptyList();
        }
        if (v1 == v2 || (v1.isEmpty() && v2.isEmpty())) {
            return 0;
        }
        if (v1.isEmpty()) {
            return nullsFirst ? -1 : 1;
        }
        if (v2.isEmpty()) {
            return nullsFirst ? 1 : -1;
        }

        final Iterator<?> i1 = v1.iterator();
        final Iterator<?> i2 = v2.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            final Object o1 = i1.next();
            final Object o2 = i2.next();
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
        if (!i1.hasNext() && i2.hasNext()) {
            return nullsFirst ? -1 : 1;
        }
        if (i1.hasNext() && !i2.hasNext()) {
            return nullsFirst ? 1 : -1;
        }
        return 0;
    }

    public static Limit cleanup(Limit v) {
        if (v == null) {
            return NO_LIMIT;
        }
        if (v.isUnrestricted()) {
            return NO_LIMIT;
        }
        int pos = v.value.size();
        while (pos > 0) {
            Object o = v.value.get(pos - 1);
            if (o != null) {
                break;
            }
            pos -= 1;
        }
        if (pos == v.value.size()) {
            return v;
        }
        if (pos < 1) {
            return NO_LIMIT;
        }
        return new Limit(new ArrayList<>(v.value.subList(0, pos)), v.inclusive);
    }

    public static class Limit implements Serializable {

        private static final long serialVersionUID = 1L;

        private final List<Object> value;
        private final boolean inclusive;

        public Limit(List<Object> value, boolean inclusive) {
            this.value = value;
            this.inclusive = inclusive;
        }

        public List<Object> getValue() {
            return value;
        }

        public boolean isInclusive() {
            return inclusive;
        }

        public boolean isUnrestricted() {
            return value == null || value.isEmpty();
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + Objects.hashCode(this.value);
            hash = 29 * hash + (this.inclusive ? 1 : 0);
            return hash;
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
            return Objects.equals(this.value, other.value);
        }

        @Override
        public String toString() {
            return "{" + "value=" + value + ", inclusive=" + inclusive + '}';
        }

        public int compareTo(Limit t, boolean nullsFirst) {
            int cmp = compare(this.value, t.value, nullsFirst);
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
