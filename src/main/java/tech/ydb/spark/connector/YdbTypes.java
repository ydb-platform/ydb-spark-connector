package tech.ydb.spark.connector;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB data type conversion, mapping and comparison.
 *
 * @author zinal
 */
public class YdbTypes {

    public static Object max(Object o1, Object o2) {
        if (o1==null || o1==o2) {
            return o2;
        }
        if (o2==null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable)o2).compareTo(o1) > 0 ? o2 : o1;
        }
        return o2;
    }

    public static Object min(Object o1, Object o2) {
        if (o1==null || o1==o2) {
            return o2;
        }
        if (o2==null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable)o2).compareTo(o1) < 0 ? o2 : o1;
        }
        return o2;
    }

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
        Type t = vr.getType();
        if (t.getKind().equals(Type.Kind.OPTIONAL)) {
            if (! vr.isOptionalItemPresent())
                return null;
            t = t.unwrapOptional();
        }
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

    public static Object convertFromYdb(Value<?> v) {
        if (v==null)
            return null;
        Type t = v.getType();
        if (t.getKind().equals(Type.Kind.OPTIONAL)) {
            t = t.unwrapOptional();
            OptionalValue ov = v.asOptional();
            if (ov.isPresent())
                v = ov.get();
            else
                return null;
        }
        switch (t.getKind()) {
            case PRIMITIVE:
                switch ((PrimitiveType)t) {
                    case Bool:
                        return v.asData().getBool();
                    case Bytes:
                        return v.asData().getBytes();
                    case Date:
                        return (int) v.asData().getDate().toEpochDay();
                    case Datetime:
                        return v.asData().getDatetime()
                                .toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
                    case Double:
                        return v.asData().getDouble();
                    case Float:
                        return v.asData().getFloat();
                    case Int16:
                        return v.asData().getInt16();
                    case Int32:
                        return v.asData().getInt32();
                    case Int64:
                        return v.asData().getInt64();
                    case Int8:
                        return (short) v.asData().getInt8();
                    case Uint8:
                        return (short) v.asData().getUint8();
                    case Json:
                        return UTF8String.fromString(v.asData().getJson());
                    case JsonDocument:
                        return UTF8String.fromString(v.asData().getJsonDocument());
                    case Text:
                        return UTF8String.fromString(v.asData().getText());
                    case Timestamp:
                        return v.asData().getTimestamp().toEpochMilli() * 1000L;
                    case Uint16:
                        return v.asData().getUint16();
                    case Uint32:
                        return v.asData().getUint32();
                    case Uint64:
                        return v.asData().getUint64();
                    case Yson:
                        return v.asData().getYson();
                }
                break;
            case DECIMAL:
                return Decimal.apply(((DecimalValue)v).toBigDecimal());
        }
        return null;
    }

    public static Value<?> convertToYdb(Object v, YdbFieldType t) {
        switch (t) {
            case Bool:
                if (v==null) {
                    return PrimitiveType.Bool.makeOptional().emptyValue();
                }
                if (v instanceof Boolean) {
                    return PrimitiveValue.newBool((Boolean) v);
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newBool( ((Number) v).intValue() != 0 );
                }
                throw badConversion(v, t);
            case Bytes:
                if (v==null) {
                    return PrimitiveType.Bytes.makeOptional().emptyValue();
                }
                if (v instanceof byte[]) {
                    return PrimitiveValue.newBytes((byte[]) v);
                }
                throw badConversion(v, t);
            case Date:
                if (v==null) {
                    return PrimitiveType.Date.makeOptional().emptyValue();
                }
                if (v instanceof java.sql.Date) {
                    return PrimitiveValue.newDate(((java.sql.Date)v).toLocalDate());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newDate(LocalDate.ofEpochDay(((Number)v).longValue()));
                }
                try {
                    if (v instanceof String || v instanceof UTF8String) {
                        return PrimitiveValue.newDate(LocalDate.parse(v.toString()));
                    }
                } catch(DateTimeParseException dtpe) {}
                throw badConversion(v, t);
            case Datetime:
                if (v==null) {
                    return PrimitiveType.Datetime.makeOptional().emptyValue();
                }
                if (v instanceof java.sql.Timestamp) {
                    return PrimitiveValue.newDatetime(((java.sql.Timestamp)v).toInstant());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newDatetime(
                            Instant.ofEpochMilli(((Number)v).longValue()));
                }
                try {
                    if (v instanceof String || v instanceof UTF8String) {
                        return PrimitiveValue.newDatetime(LocalDateTime.parse(v.toString()));
                    }
                } catch(DateTimeParseException dtpe) {}
                throw badConversion(v, t);
            case Timestamp:
                if (v==null) {
                    return PrimitiveType.Timestamp.makeOptional().emptyValue();
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newTimestamp(
                            Instant.ofEpochMilli(((Number)v).longValue()));
                }
                try {
                    if (v instanceof String || v instanceof UTF8String) {
                        return PrimitiveValue.newTimestamp(Instant.parse(v.toString()));
                    }
                } catch(DateTimeParseException dtpe) {}
                throw badConversion(v, t);
            case Decimal:
                if (v==null) {
                    return DecimalType.getDefault().makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return DecimalType.getDefault().newValue(((Decimal)v).toJavaBigDecimal());
                }
                if (v instanceof BigDecimal) {
                    return DecimalType.getDefault().newValue(((BigDecimal)v));
                }
                if (v instanceof Double) {
                    return DecimalType.getDefault().newValue(new BigDecimal((Double)v));
                }
                if (v instanceof Float) {
                    return DecimalType.getDefault().newValue(new BigDecimal((Float)v));
                }
                if (v instanceof Number) {
                    return DecimalType.getDefault().newValue(new BigDecimal(((Number)v).longValue()));
                }
                throw badConversion(v, t);
            case Double:
                if (v==null) {
                    return PrimitiveType.Double.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newDouble(((Decimal)v).toJavaBigDecimal().doubleValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newDouble(((BigDecimal)v).doubleValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newDouble(((Number)v).doubleValue());
                }
                throw badConversion(v, t);
            case Float:
                if (v==null) {
                    return PrimitiveType.Float.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newFloat(((Decimal)v).toJavaBigDecimal().floatValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newFloat(((BigDecimal)v).floatValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newFloat(((Number)v).floatValue());
                }
                throw badConversion(v, t);
            case Int16:
                if (v==null) {
                    return PrimitiveType.Int16.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newInt16(((Decimal)v).toJavaBigDecimal().shortValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newInt16(((BigDecimal)v).shortValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newInt16(((Number)v).shortValue());
                }
                throw badConversion(v, t);
            case Int32:
                if (v==null) {
                    return PrimitiveType.Int32.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newInt32(((Decimal)v).toJavaBigDecimal().intValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newInt32(((BigDecimal)v).intValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newInt32(((Number)v).intValue());
                }
                throw badConversion(v, t);
            case Int64:
                if (v==null) {
                    return PrimitiveType.Int64.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newInt64(((Decimal)v).toJavaBigDecimal().longValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newInt64(((BigDecimal)v).longValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newInt64(((Number)v).longValue());
                }
                throw badConversion(v, t);
            case Int8:
                if (v==null) {
                    return PrimitiveType.Int8.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newInt8(((Decimal)v).toJavaBigDecimal().byteValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newInt8(((BigDecimal)v).byteValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newInt8(((Number)v).byteValue());
                }
                throw badConversion(v, t);
            case Uint16:
                if (v==null) {
                    return PrimitiveType.Uint16.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newUint16(((Decimal)v).toJavaBigDecimal().intValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newUint16(((BigDecimal)v).intValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newUint16(((Number)v).intValue());
                }
                throw badConversion(v, t);
            case Uint32:
                if (v==null) {
                    return PrimitiveType.Uint32.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newUint32(((Decimal)v).toJavaBigDecimal().longValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newUint32(((BigDecimal)v).longValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newUint32(((Number)v).longValue());
                }
                throw badConversion(v, t);
            case Uint64:
                if (v==null) {
                    return PrimitiveType.Uint64.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newUint64(((Decimal)v).toJavaBigDecimal().longValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newUint64(((BigDecimal)v).longValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newUint64(((Number)v).longValue());
                }
                throw badConversion(v, t);
            case Uint8:
                if (v==null) {
                    return PrimitiveType.Uint8.makeOptional().emptyValue();
                }
                if (v instanceof Decimal) {
                    return PrimitiveValue.newUint8(((Decimal)v).toJavaBigDecimal().intValue());
                }
                if (v instanceof BigDecimal) {
                    return PrimitiveValue.newUint8(((BigDecimal)v).intValue());
                }
                if (v instanceof Number) {
                    return PrimitiveValue.newUint8(((Number)v).intValue());
                }
                throw badConversion(v, t);
            case Text:
                if (v==null) {
                    return PrimitiveType.Text.makeOptional().emptyValue();
                }
                return PrimitiveValue.newText(v.toString());
            default:
                throw new IllegalArgumentException("Conversion to type " + t + " is not supported");
        }
    }

    private static UnsupportedOperationException badConversion(Object v, YdbFieldType t) {
        throw new UnsupportedOperationException("Cannot convert value [" + v + "] of class " +
                v.getClass().getName() + " to type " + t);
    }

}

