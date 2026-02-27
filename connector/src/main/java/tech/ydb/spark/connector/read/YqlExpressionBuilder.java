package tech.ydb.spark.connector.read;

import java.util.StringJoiner;

import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlExpressionBuilder extends V2ExpressionSQLBuilder {
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

    @Override
    protected String visitStartsWith(String left, String right) {
        return "STARTSWITH(" + left + "," + right + ")";
    }

    @Override
    protected String visitEndsWith(String left, String right) {
        return "ENDSWITH(" + left + "," + right + ")";
    }

    @Override
    protected String visitContains(String left, String right) {
        return "FIND(" + left + "," + right + ") IS NOT NULL";
    }

    @Override
    protected String visitLiteral(Literal<?> literal) {
        if (literal.dataType().sameType(DataTypes.BinaryType) && literal.value() instanceof byte[]) {
            byte[] bytes = (byte[]) literal.value();
            StringBuilder sb = new StringBuilder("\"");
            for (byte b: bytes) {
                sb.append("\\x").append(HEX_ARRAY[b >>> 4]).append(HEX_ARRAY[b & 0x0F]);
            }
            sb.append("\"");
            return sb.toString();
        }
        return literal.toString();
    }

    @Override
    protected String visitSQLFunction(String funcName, String[] inputs) {
        if ("SUBSTRING".equals(funcName) && (inputs.length == 2 || inputs.length == 3)) {
            StringJoiner joiner = new StringJoiner(", ", "SUBSTRING(", ")");
            joiner.add("CAST(" + inputs[0] + " AS  String)");
            joiner.add("CAST((" + inputs[1] + " - 1) AS UInt32)");
            if (inputs.length == 3) {
                joiner.add(inputs[2]);
            }
            return joiner.toString();
        }

        return super.visitSQLFunction(funcName, inputs);
    }
}
