package tech.ydb.spark.connector.read;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlExpressionBuilder extends V2ExpressionSQLBuilder {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);

    @Override
    protected String visitLiteral(Literal<?> literal) {
        if (DataTypes.DateType.sameType(literal.dataType())) {
            Object v = literal.value();
            if (v == null) {
                return "NULL";
            }
            long days = ((Number) v).longValue();
            LocalDate date = LocalDate.ofEpochDay(days);
            return "Date(\"" + date + "\")";
        }
        if (DataTypes.TimestampType.sameType(literal.dataType())) {
            Object v = literal.value();
            if (v == null) {
                return "NULL";
            }
            long micros = ((Number) v).longValue();
            Instant instant = Instant.ofEpochSecond(
                    micros / 1_000_000, (micros % 1_000_000) * 1000);
            return "Timestamp(\"" + TIMESTAMP_FORMAT.format(instant) + "\")";
        }
        return super.visitLiteral(literal);
    }

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
