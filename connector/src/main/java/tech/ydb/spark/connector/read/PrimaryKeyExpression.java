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
public class PrimaryKeyExpression implements YdbPartition {
    private static final long serialVersionUID = 3138686052261512975L;

    private final String expression;
    private final String valueName;
    private final Value<?> value;

    private PrimaryKeyExpression(FieldInfo[] columns, TupleValue value, String valueName, String operation) {
        StringBuilder ex = new StringBuilder();
        if (value.size() > 1) {
            ex.append("(");
        }
        String del = "";
        for (int idx = 0; idx < columns.length && idx < value.size(); idx++) {
            ex.append(del);
            ex.append('`').append(columns[idx].getName()).append('`');
            del = ", ";
        }
        if (value.size() > 1) {
            ex.append(")");
        }
        ex.append(" ").append(operation).append(" ").append(valueName);

        this.expression = ex.toString();
        this.valueName = valueName;
        this.value = value.size() > 1 ? value : value.get(0);
    }

    @Override
    public SelectQuery makeQuery(SelectQuery origin) {
        return origin.addExpressionWithParam(expression, valueName, value);
    }

    public static PrimaryKeyExpression keyRangeFrom(YdbTypes types, FieldInfo[] columns, KeysRange keyRange) {
        if (!keyRange.hasFromValue()) {
            return null;
        }

        TupleValue fromValue = keyRange.readFromValue(types, columns);
        return new PrimaryKeyExpression(columns, fromValue, "$f", keyRange.includesFromValue() ? ">=" : ">");
    }

    public static PrimaryKeyExpression keyRangeTo(YdbTypes types, FieldInfo[] columns, KeysRange keyRange) {
        if (!keyRange.hasToValue()) {
            return null;
        }

        TupleValue toValue = keyRange.readToValue(types, columns);
        return new PrimaryKeyExpression(columns, toValue, "$t", keyRange.includesToValue() ? "<=" : "<");
    }
}
