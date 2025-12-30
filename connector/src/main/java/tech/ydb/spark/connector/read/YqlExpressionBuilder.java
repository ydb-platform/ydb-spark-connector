package tech.ydb.spark.connector.read;

import java.util.StringJoiner;

import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlExpressionBuilder extends V2ExpressionSQLBuilder {

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
    protected String visitSQLFunction(String funcName, String[] inputs) {
        if ("SUBSTRING".equals(funcName) && (inputs.length == 2 || inputs.length == 3)) {
            StringJoiner joiner = new StringJoiner(", ", "Unicode::Substring(", ")");
            joiner.add(inputs[0]);
            joiner.add(inputs[1] + " - 1");
            if (inputs.length == 3) {
                joiner.add(inputs[2]);
            }
            return joiner.toString();
        }

        return super.visitSQLFunction(funcName, inputs);
    }
}
