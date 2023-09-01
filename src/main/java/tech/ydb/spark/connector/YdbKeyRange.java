package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import tech.ydb.table.description.KeyBound;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class YdbKeyRange implements Serializable {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbKeyRange.class);

    private final Limit from;
    private final Limit to;

    public YdbKeyRange(Limit from, Limit to) {
        this.from = (from==null) ? NO_LIMIT : from;
        this.to = (to==null) ? NO_LIMIT : to;
    }

    public YdbKeyRange(KeyRange kr) {
        this(convert(kr.getFrom()), convert(kr.getTo()));
    }

    public Limit getFrom() {
        return from;
    }

    public Limit getTo() {
        return to;
    }

    public boolean isEmpty() {
        return from.compareTo(to, true) > 0;
    }

    public static Limit convert(Optional<KeyBound> v) {
        if (v.isPresent()) {
            KeyBound kb = v.get();
            Value<?> tx = kb.getValue();
            if (! (tx instanceof TupleValue) ) {
                throw new IllegalArgumentException();
            }
            TupleValue t = (TupleValue) tx;
            final int sz = t.size();
            List<Object> out = new ArrayList<>(sz);
            for (int i=0; i<sz; ++i) {
                out.add(YdbTypes.convertFromYdb(t.get(i)));
            }
            return new Limit(out, kb.isInclusive());
        }
        return null;
    }

    @Override
    public String toString() {
        return  (from.isInclusive() ? "[* " : "(* ")
                + from.value + " -> " + to.value
                + (to.isInclusive() ? " *]" : " *)");
    }

    public YdbKeyRange intersect(YdbKeyRange other) {
        if (other==null ||
                (other.from.isUnrestricted() && other.to.isUnrestricted())) {
            LOG.debug("intersect: {} with unrestricted", this);
            return this;
        }
        if ( from.isUnrestricted() && to.isUnrestricted() ) {
            LOG.debug("intersect: unrestricted with {}", other);
            return other;
        }
        Limit outFrom = (from.compareTo(other.from, true) > 0) ? from : other.from;
        Limit outTo = (to.compareTo(other.to, true) > 0) ? other.to : to;
        YdbKeyRange retval = new YdbKeyRange(outFrom, outTo);
        LOG.debug("intersect: {} with {} -> {}", this, other, retval);
        return retval;
    }

    public static int compare(List<Object> v1, List<Object> v2, boolean nullsFirst) {
        if (v1==null)
            v1 = Collections.emptyList();
        if (v2==null)
            v2 = Collections.emptyList();
        if (v1==v2 || (v1.isEmpty() && v2.isEmpty()))
            return 0;
        if (v1.isEmpty())
            return nullsFirst ? -1 : 1;
        if (v2.isEmpty())
            return nullsFirst ? 1 : -1;

        final Iterator<?> i1 = v1.iterator(), i2 = v2.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            final Object o1 = i1.next();
            final Object o2 = i2.next();
            if (o1==o2)
                continue;
            if (o1==null)
                return nullsFirst ? -1 : 1;
            if (o2==null)
                return nullsFirst ? 1 : -1;
            if (o1.getClass()!=o2.getClass()) {
                throw new IllegalArgumentException("Incompatible data types "
                        + o1.getClass().toString() + " and " + o2.getClass().toString());
            }
            if (!(o1 instanceof Comparable)) {
                throw new IllegalArgumentException("Uncomparable data type "
                        + o1.getClass().toString());
            }
            final int cmp = ((Comparable)o1).compareTo(o2);
            if (cmp != 0) return cmp;
        }
        if (!i1.hasNext() && i2.hasNext()) return nullsFirst ? -1 : 1;
        if (i1.hasNext() && !i2.hasNext()) return nullsFirst ? 1 : -1;
        return 0;
    }

    public static class Limit implements Serializable {
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
            return value==null || value.isEmpty();
        }

        @Override
        public String toString() {
            return "{" + "value=" + value + ", inclusive=" + inclusive + '}';
        }

        public int compareTo(Limit t, boolean nullsFirst) {
            int cmp = compare(this.value, t.value, nullsFirst);
            if (cmp==0) {
                if (this.inclusive == t.inclusive)
                    return 0;
                if (this.inclusive)
                    return nullsFirst ? -1 : 1;
                return nullsFirst ? 1 : -1;
            }
            return cmp;
        }
    }

    public static final Limit NO_LIMIT = new Limit(new ArrayList<>(), true);
    public static final YdbKeyRange UNRESTRICTED = new YdbKeyRange(NO_LIMIT, NO_LIMIT);

}
