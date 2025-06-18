package tech.ydb.spark.connector.read;


import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.KeysRange;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbPartitions {
    private YdbPartitions() { }

    public static YdbPartition none() {
        return new Unrestricted();
    }

    public static YdbPartition tabletId(String id) {
        return new TabletId(id);
    }

    public static YdbPartition keysRange(YdbTypes types, FieldInfo[] columns, KeysRange keyRange) {
        if (columns.length == 0 || keyRange.isUnrestricted()) {
            return new Unrestricted();
        }

        if (keyRange.isEmpty()) {
            return new Empty();
        }

        StringBuilder keys = new StringBuilder("");
        String del = "";
        for (FieldInfo fi: columns) {
            keys.append(del);
            keys.append('`').append(fi.getName()).append('`');
            del = ", ";
        }

        TupleValue from = keyRange.hasFromValue() ? keyRange.readFromValue(types, columns) : null;
        TupleValue to = keyRange.hasToValue() ? keyRange.readToValue(types, columns) : null;
        boolean includeFrom = keyRange.includesFromValue();
        boolean includeTo = keyRange.includesToValue();

        String cols = keys.toString();
        if (columns.length == 1) {
            Value<?> fromValue = from != null ? from.get(0) : null;
            Value<?> toValue = to != null ? to.get(0) : null;
            return new KeyRangeShard(cols, fromValue, toValue, includeFrom, includeTo);
        }

        return new KeyRangeShard("(" + cols + ")", from, to, includeFrom, includeTo);
    }

    private static class Unrestricted implements YdbPartition {
        private static final long serialVersionUID = 2842737463454106189L;

        @Override
        public SelectQuery makeQuery(SelectQuery origin) {
            return origin.copy();
        }
    }

    private static class Empty implements YdbPartition {
        private static final long serialVersionUID = 8062207972148236947L;

        @Override
        public SelectQuery makeQuery(SelectQuery origin) {
            return origin.copy().addExpression("1 = 0"); // disable all
        }
    }

    private static class TabletId implements YdbPartition {
        private static final long serialVersionUID = 8869357112576530212L;

        private final String tabletId;

        TabletId(String tabletId) {
            this.tabletId = tabletId;
        }

        @Override
        public SelectQuery makeQuery(SelectQuery origin) {
            return origin.copy().setWithExpression("TabletId='" + tabletId + "'");
        }
    }

    private static class KeyRangeShard implements YdbPartition {
        private static final long serialVersionUID = -8344489162515121573L;

        private final String columns;
        private final Value<?> from;
        private final Value<?> to;
        private final boolean includeFrom;
        private final boolean includeTo;

        KeyRangeShard(String columns, Value<?> from, Value<?> to, boolean includeFrom, boolean includeTo) {
            this.columns = columns;
            this.from = from;
            this.to = to;
            this.includeFrom = includeFrom;
            this.includeTo = includeTo;
        }

        @Override
        public SelectQuery makeQuery(SelectQuery origin) {
            SelectQuery query = origin.copy();
            if (from != null) {
                String exp = columns + (includeFrom ? " >= " : " > ") + "$f";
                query = query.addExpressionWithParam(exp, "$f", from);
            }
            if (to != null) {
                String exp = columns + (includeTo ? " <= " : " < ") + "$t";
                query = query.addExpressionWithParam(exp, "$t", to);
            }
            return query;
        }
    }
}
