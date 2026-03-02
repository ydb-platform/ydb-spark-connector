package tech.ydb.spark.connector.read;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
            return "Date32(\"" + LocalDate.ofEpochDay(days) + "\")";
        }

        if (DataTypes.TimestampType.sameType(literal.dataType())) {
            Object v = literal.value();
            if (v == null) {
                return "NULL";
            }
            long micros = ((Number) v).longValue();
            long seconds = micros / 1_000_000;
            long nanos = 1000 * (micros - seconds * 1_000_000);
            return "Timestamp64(\"" + TIMESTAMP_FORMAT.format(Instant.ofEpochSecond(seconds, nanos)) + "\")";
        }

        if (literal.dataType().sameType(DataTypes.BinaryType) && literal.value() instanceof byte[]) {
            byte[] bytes = (byte[]) literal.value();
            StringBuilder sb = new StringBuilder("\"");
            for (byte b: bytes) {
                sb.append("\\x").append(HEX_ARRAY[(b & 0xF0) >>> 4]).append(HEX_ARRAY[b & 0x0F]);
            }
            sb.append("\"");
            return sb.toString();
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

    @Override
    protected String visitSQLFunction(String funcName, String[] inputs) {
        if ("SUBSTRING".equals(funcName) && (inputs.length == 2 || inputs.length == 3)) {
            StringJoiner joiner = new StringJoiner(", ", "SUBSTRING(", ")");
            joiner.add("CAST(" + inputs[0] + " AS String)");
            joiner.add("CAST((" + inputs[1] + " - 1) AS UInt32)");
            if (inputs.length == 3) {
                joiner.add(inputs[2]);
            }
            return joiner.toString();
        }

        return super.visitSQLFunction(funcName, inputs);
    }
}
