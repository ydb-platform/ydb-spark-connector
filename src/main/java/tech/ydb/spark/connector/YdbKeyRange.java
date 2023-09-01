package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.ArrayList;
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

    private final Limit from;
    private final Limit to;

    public YdbKeyRange(Limit from, Limit to) {
        this.from = from;
        this.to = to;
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
        return "KeyRange{" + from + " -> " + to + '}';
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

        @Override
        public String toString() {
            return "{" + "value=" + value + ", inclusive=" + inclusive + '}';
        }
    }

}
