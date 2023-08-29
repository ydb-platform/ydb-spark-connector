package tech.ydb.spark.connector;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class YdbTypes {

    public static boolean mapNullable(tech.ydb.table.values.Type yt) {
        switch (yt.getKind()) {
            case OPTIONAL:
                return true;
            default:
                return false;
        }
    }

    public static DataType mapType(tech.ydb.table.values.Type yt) {
        if (yt==null)
            return null;
        switch (yt.getKind()) {
            case OPTIONAL:
                yt = yt.unwrapOptional();
                break;
        }
        switch (yt.getKind()) {
            case PRIMITIVE:
                break;
            case DECIMAL:
                return DataTypes.createDecimalType(38, 10);
            default:
                return null;
        }
        PrimitiveType pt = (PrimitiveType) yt;
        switch (pt) {
            case Bool:
                return DataTypes.BooleanType;
            case Int8:
                return DataTypes.ByteType;
            case Uint8:
                return DataTypes.ShortType;
            case Int16:
                return DataTypes.ShortType;
            case Uint16:
                return DataTypes.IntegerType;
            case Int32:
                return DataTypes.IntegerType;
            case Uint32:
                return DataTypes.LongType;
            case Int64:
                return DataTypes.LongType;
            case Uint64:
                return DataTypes.LongType;
            case Float:
                return DataTypes.FloatType;
            case Double:
                return DataTypes.DoubleType;
            case Bytes:
                return DataTypes.BinaryType;
            case Text:
                return DataTypes.StringType;
            case Yson:
                return DataTypes.StringType;
            case Json:
                return DataTypes.StringType;
            case Uuid:
                return DataTypes.StringType;
            case Date:
                return DataTypes.DateType;
            case Datetime:
                return DataTypes.TimestampType;
            case Timestamp:
                return DataTypes.TimestampType;
            case Interval:
                return DataTypes.CalendarIntervalType;
            case TzDate:
                return DataTypes.DateType;
            case TzDatetime:
                return DataTypes.TimestampType;
            case TzTimestamp:
                return DataTypes.TimestampType;
            case JsonDocument:
                return DataTypes.StringType;
            case DyNumber:
                return DataTypes.createDecimalType(38, 10);
        }
        return null;
    }

    public static Object convertFromYdb(ValueReader vr) {
        if (vr==null)
            return null;
        if (vr.getType().getKind().equals(Type.Kind.OPTIONAL)) {
            if (! vr.isOptionalItemPresent())
                return null;
        }
        Type t = vr.getType().unwrapOptional();
        switch (t.getKind()) {
            case PRIMITIVE:
                switch ((PrimitiveType)t) {
                    case Bool:
                        return vr.getBool();
                    case Bytes:
                        return vr.getBytes();
                    case Date:
                        return (int) vr.getDate().toEpochDay();
                    case Datetime:
                        return vr.getDatetime().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
                    case Double:
                        return vr.getDouble();
                    case Float:
                        return vr.getFloat();
                    case Int16:
                        return vr.getInt16();
                    case Int32:
                        return vr.getInt32();
                    case Int64:
                        return vr.getInt64();
                    case Int8:
                        return (short) vr.getInt8();
                    case Uint8:
                        return (short) vr.getUint8();
                    case Json:
                        return UTF8String.fromString(vr.getJson());
                    case JsonDocument:
                        return UTF8String.fromString(vr.getJsonDocument());
                    case Text:
                        return UTF8String.fromString(vr.getText());
                    case Timestamp:
                        return vr.getTimestamp().toEpochMilli() * 1000L;
                    case Uint16:
                        return vr.getUint16();
                    case Uint32:
                        return vr.getUint32();
                    case Uint64:
                        return vr.getUint64();
                    case Uuid:
                        return vr.getUuid();
                    case Yson:
                        return vr.getYson();
                }
                break;
            case DECIMAL:
                return Decimal.apply(vr.getDecimal().toBigDecimal());
        }
        return null;
    }

    public static Value<?> convertToYdb(Object x) {
        if (x instanceof String) {
            return PrimitiveValue.newText(x.toString());
        }
        if (x instanceof UTF8String) {
            return PrimitiveValue.newText(x.toString());
        }
        if (x instanceof Integer) {
            return PrimitiveValue.newInt32((Integer)x);
        }
        if (x instanceof Short) {
            return PrimitiveValue.newInt32((Short)x);
        }
        if (x instanceof Long) {
            return PrimitiveValue.newInt64((Long)x);
        }
        if (x instanceof byte[]) {
            return PrimitiveValue.newBytes((byte[])x);
        }
        if (x instanceof Double) {
            return PrimitiveValue.newDouble((Double)x);
        }
        if (x instanceof Float) {
            return PrimitiveValue.newFloat((Float)x);
        }
        if (x instanceof BigDecimal) {
            return DecimalType.getDefault().newValue((BigDecimal)x);
        }
        if (x instanceof Decimal) {
            return DecimalType.getDefault().newValue( ((Decimal)x).toJavaBigDecimal() );
        }
        if (x instanceof Boolean) {
            return PrimitiveValue.newBool((Boolean)x);
        }
        if (x instanceof java.sql.Timestamp) {
            return PrimitiveValue.newTimestamp(((java.sql.Timestamp)x).toInstant());
        }
        if (x instanceof java.sql.Date) {
            return PrimitiveValue.newDate(((java.sql.Date)x).toLocalDate());
        }
        throw new IllegalArgumentException(x.getClass().getName());
    }

}

