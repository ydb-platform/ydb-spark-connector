package tech.ydb.spark.connector.read;

import org.apache.spark.sql.connector.read.InputPartition;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.KeysRange;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbPartition extends InputPartition {
    SelectQuery makeQuery(SelectQuery origin);

    static YdbPartition none() {
        return new YdbPartition() {
            private static final long serialVersionUID = -7536076317892048979L;
            @Override
            public SelectQuery makeQuery(SelectQuery origin) {
                return origin.copy().addExpression("1 = 0"); // disable all
            }
        };
    }

    static YdbPartition unrestricted() {
        return new YdbPartition() {
            private static final long serialVersionUID = -7536076317892048980L;
            @Override
            public SelectQuery makeQuery(SelectQuery origin) {
                return origin.copy();
            }
        };
    }

    static YdbPartition tabletId(String tabletId) {
        return new YdbPartition() {
            private static final long serialVersionUID = -7536076317892048981L;
            @Override
            public SelectQuery makeQuery(SelectQuery origin) {
                return origin.copy().setWithExpression("TabletId='" + tabletId + "'");
            }
        };
    }

    static YdbPartition keysRange(YdbTypes types, FieldInfo[] columns, KeysRange keyRange) {
        if (columns.length == 0 || keyRange.isUnrestricted()) {
            return unrestricted();
        }

        if (keyRange.isEmpty()) {
            return none();
        }

        final PrimaryKeyExpression from = PrimaryKeyExpression.keyRangeFrom(types, columns, keyRange);
        final PrimaryKeyExpression to = PrimaryKeyExpression.keyRangeTo(types, columns, keyRange);

        return new YdbPartition() {
            private static final long serialVersionUID = -7536076317892048982L;
            @Override
            public SelectQuery makeQuery(SelectQuery origin) {
                SelectQuery query = origin.copy();
                if (from != null) {
                    query = from.makeQuery(origin);
                }
                if (to != null) {
                    query = to.makeQuery(origin);
                }
                return query;
            }
        };
    }
}
