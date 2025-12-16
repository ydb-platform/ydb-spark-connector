package tech.ydb.spark.connector.read;

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
}
