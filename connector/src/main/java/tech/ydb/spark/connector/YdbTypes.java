package tech.ydb.spark.connector;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import com.google.common.primitives.UnsignedBytes;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;

import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.OptionalType;
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
public final class YdbTypes implements Serializable {
    private static final long serialVersionUID = 20250904001L;

    public static final DataType SPARK_DECIMAL = DataTypes.createDecimalType(38, 10);
    public static final DataType SPARK_UINT64 = DataTypes.createDecimalType(22, 0);

    private final boolean dateAsString;
    private final boolean useSignedDatetypes;

    public YdbTypes(CaseInsensitiveStringMap options) {
        this.dateAsString = OperationOption.DATE_AS_STRING.readBoolean(options, false);
        this.useSignedDatetypes = OperationOption.TABLE_USE_SIGNED_DATETYPES.readBoolean(options, false);
    }

    private boolean mapNullable(tech.ydb.table.values.Type yt) {
        switch (yt.getKind()) {
            case OPTIONAL:
                return true;
            default:
                return false;
        }
    }

    public FieldInfo fromSparkField(StructField field) {
        Type type = mapTypeSpark2Ydb(field.dataType());
        if (type == null) {
            throw new IllegalArgumentException("Unsupported type for table column: " + field.dataType());
        }
        if (field.nullable()) {
            type = type.makeOptional();
        }
        return new FieldInfo(field.name(), type);
    }

    public List<FieldInfo> fromSparkSchema(StructType schema) {
        final List<FieldInfo> fields = new ArrayList<>(schema.size());
        for (StructField field : schema.fields()) {
            fields.add(fromSparkField(field));
        }
        return fields;
    }

    public StructType toSparkSchema(List<TableColumn> columns) {
        final List<StructField> fields = new ArrayList<>();
        for (TableColumn tc : columns) {
            DataType dataType = mapTypeYdb2Spark(tc.getType());
            if (dataType != null) {
                fields.add(new StructField(tc.getName(), dataType, mapNullable(tc.getType()), Metadata.empty()));
            }
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    public StructType toSparkSchema(ResultSetReader rs) {
        final List<StructField> fields = new ArrayList<>();
        for (int idx = 0; idx < rs.getColumnCount(); idx += 1) {
            String name = rs.getColumnName(idx);
            Type type = rs.getColumnType(idx);
            DataType dataType = mapTypeYdb2Spark(type);
            if (dataType != null) {
                fields.add(new StructField(name, dataType, mapNullable(type), Metadata.empty()));
            }
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    private DataType mapTypeYdb2Spark(Type yt) {
        if (yt == null) {
            return null;
        }
        switch (yt.getKind()) {
            case OPTIONAL:
                yt = yt.unwrapOptional();
                break;
            default: {
                /* noop */
            }
        }
        switch (yt.getKind()) {
            case PRIMITIVE:
                break;
            case DECIMAL: {
                DecimalType dt = (DecimalType) yt;
                return DataTypes.createDecimalType(dt.getPrecision(), dt.getScale());
            }
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
                return SPARK_UINT64;
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
            case JsonDocument:
                return DataTypes.StringType;
            case Uuid:
                return DataTypes.StringType;
            case Date:
            case Date32:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.DateType;
            case Datetime:
            case Datetime64:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.TimestampType;
            case Timestamp:
            case Timestamp64:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.TimestampType;
            case Interval:
            case Interval64:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.CalendarIntervalType;
            case TzDate:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.DateType;
            case TzDatetime:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.TimestampType;
            case TzTimestamp:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.TimestampType;
            case DyNumber:
                return SPARK_DECIMAL;
            default: {
                /* noop */
            }
        }
        return null;
    }

    public Type mapTypeSpark2Ydb(org.apache.spark.sql.types.DataType type) {
        if (type instanceof org.apache.spark.sql.types.DecimalType) {
            org.apache.spark.sql.types.DecimalType x = (org.apache.spark.sql.types.DecimalType) type;
            if (x.scale() == 0) {
                if (x.precision() <= 21) {
                    return PrimitiveType.Int64;
                }
            }
            if (x.scale() == 38 && x.precision() == 10) {
                return PrimitiveType.DyNumber;
            }
            return DecimalType.of(x.precision(), x.scale());
        }
        if (type instanceof org.apache.spark.sql.types.VarcharType) {
            return PrimitiveType.Text;
        }
        if (DataTypes.BooleanType.sameType(type)) {
            return PrimitiveType.Bool;
        }
        if (DataTypes.ByteType.sameType(type)) {
            return PrimitiveType.Int8;
        }
        if (DataTypes.ShortType.sameType(type)) {
            return PrimitiveType.Int16;
        }
        if (DataTypes.IntegerType.sameType(type)) {
            return PrimitiveType.Int32;
        }
        if (DataTypes.LongType.sameType(type)) {
            return PrimitiveType.Int64;
        }
        if (DataTypes.FloatType.sameType(type)) {
            return PrimitiveType.Float;
        }
        if (DataTypes.DoubleType.sameType(type)) {
            return PrimitiveType.Double;
        }
        if (DataTypes.BinaryType.sameType(type)) {
            return PrimitiveType.Bytes;
        }
        if (DataTypes.StringType.sameType(type)) {
            return PrimitiveType.Text;
        }
        if (DataTypes.DateType.sameType(type)) {
            return useSignedDatetypes ? PrimitiveType.Date32 : PrimitiveType.Date;
        }
        if (DataTypes.TimestampType.sameType(type)) {
            return useSignedDatetypes ? PrimitiveType.Timestamp64 : PrimitiveType.Timestamp;
        }
        if (DataTypes.CalendarIntervalType.sameType(type)) {
            return useSignedDatetypes ? PrimitiveType.Interval64 : PrimitiveType.Interval;
        }
        return null;
    }

    /**
     * Conversion from YDB value to the comparable and serializable format.
     * Currently used in the KeysRange class only to perform key-to-key operations.
     *
     * @param v YDB value
     * @return POJO value supporting Serializable and Comparable interfaces
     */
    public Serializable ydb2pojo(Value<?> v) {
        if (v == null) {
            return null;
        }
        Type t = v.getType();
        if (t.getKind().equals(Type.Kind.OPTIONAL)) {
            t = t.unwrapOptional();
            OptionalValue ov = v.asOptional();
            if (ov.isPresent()) {
                v = ov.get();
            } else {
                return null;
            }
        }
        switch (t.getKind()) {
            case PRIMITIVE:
                switch ((PrimitiveType) t) {
                    case Bool:
                        return v.asData().getBool();
                    case Bytes:
                        return new Bytes(v.asData().getBytes());
                    case Date:
                        if (dateAsString) {
                            return UTF8String.fromString(v.asData().getDate().toString());
                        }
                        return (int) v.asData().getDate().toEpochDay();
                    case Datetime:
                        if (dateAsString) {
                            return UTF8String.fromString(v.asData().getDatetime().toString());
                        }
                        return v.asData().getDatetime()
                                .toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
                    case Timestamp:
                        if (dateAsString) {
                            return UTF8String.fromString(v.asData().getTimestamp().toString());
                        }
                        return v.asData().getTimestamp().toEpochMilli() * 1000L;
                    case Interval:
                        return v.asData().getInterval();
                    case Date32:
                        if (dateAsString) {
                            return UTF8String.fromString(v.asData().getDate32().toString());
                        }
                        return (int) v.asData().getDate32().toEpochDay();
                    case Datetime64:
                        if (dateAsString) {
                            return UTF8String.fromString(v.asData().getDatetime64().toString());
                        }
                        return v.asData().getDatetime64()
                                .toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
                    case Timestamp64:
                        if (dateAsString) {
                            return UTF8String.fromString(v.asData().getTimestamp64().toString());
                        }
                        return v.asData().getTimestamp64().toEpochMilli() * 1000L;
                    case Interval64:
                        return v.asData().getInterval64();
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
                    case Uint16:
                        return v.asData().getUint16();
                    case Uint32:
                        return v.asData().getUint32();
                    case Uint64:
                        return Decimal.apply(new BigDecimal(v.asData().toString()));
                    case Yson:
                        return v.asData().getYson();
                    default: {
                        /* noop */
                    }
                }
                break;
            case DECIMAL:
                return Decimal.apply(((DecimalValue) v).toBigDecimal());
            default: {
                /* noop */
            }
        }
        return null;
    }

    public Value<?> convertToYdb(Object v, Type t) {
        if (v == null) {
            if (t.getKind() == Type.Kind.OPTIONAL) {
                return ((OptionalType) t).emptyValue();
            }
            throw new UnsupportedOperationException(
                    "Cannot cast NULL to non-nullable type " + t);
        }

        switch (t.getKind()) {
            case OPTIONAL:
                return convertToYdb(v, t.unwrapOptional());
            case DECIMAL:
                return convertDecimalToYdb(v, (DecimalType) t);
            case PRIMITIVE:
                return convertPrimitiveToYdb(v, (PrimitiveType) t);
            default:
                throw new IllegalArgumentException("Conversion to type " + t + " is not supported");
        }
    }

    private Value<?> convertPrimitiveToYdb(Object v, PrimitiveType t) {
        switch (t) {
            case Bool:
                return convertBoolToYdb(v, t);
            case Bytes:
                return convertBytesToYdb(v, t);
            case Date:
                return convertDateToYdb(v, t);
            case Datetime:
                return convertDatetimeToYdb(v, t);
            case Timestamp:
                return convertTimestampToYdb(v, t);
            case Date32:
                return convertDate32ToYdb(v, t);
            case Datetime64:
                return convertDatetime64ToYdb(v, t);
            case Timestamp64:
                return convertTimestamp64ToYdb(v, t);
            case Double:
                return convertDoubleToYdb(v, t);
            case Float:
                return convertFloatToYdb(v, t);
            case Int8:
                return convertInt8ToYdb(v, t);
            case Int16:
                return convertInt16ToYdb(v, t);
            case Int32:
                return convertInt32ToYdb(v, t);
            case Int64:
                return convertInt64ToYdb(v, t);
            case Uint8:
                return convertUint8ToYdb(v, t);
            case Uint16:
                return convertUint16ToYdb(v, t);
            case Uint32:
                return convertUint32ToYdb(v, t);
            case Uint64:
                return convertUint64ToYdb(v, t);
            case Text:
                return convertTextToYdb(v);
            default:
                throw new IllegalArgumentException("Conversion to type " + t + " is not supported");
        }
    }

    private Value<?> convertTextToYdb(Object v) {
        return PrimitiveValue.newText(v.toString());
    }

    private Value<?> convertUint8ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newUint8(((Decimal) v).toJavaBigDecimal().intValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newUint8(((BigDecimal) v).intValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newUint8(((Number) v).intValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newUint8(((UTF8String) v).toByteExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertUint64ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newUint64(((Decimal) v).toJavaBigDecimal().longValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newUint64(((BigDecimal) v).longValue());
        }
        if (v instanceof BigInteger) {
            long temp = Long.parseUnsignedLong(v.toString());
            return PrimitiveValue.newUint64(temp);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newUint64(((Number) v).longValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newUint64(((UTF8String) v).toLongExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertUint32ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newUint32(((Decimal) v).toJavaBigDecimal().longValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newUint32(((BigDecimal) v).longValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newUint32(((Number) v).longValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newUint32(((UTF8String) v).toIntExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertUint16ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newUint16(((Decimal) v).toJavaBigDecimal().intValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newUint16(((BigDecimal) v).intValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newUint16(((Number) v).intValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newUint16(((UTF8String) v).toShortExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertInt8ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newInt8(((Decimal) v).toJavaBigDecimal().byteValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newInt8(((BigDecimal) v).byteValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt8(((Number) v).byteValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newInt8(((UTF8String) v).toByteExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertInt64ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newInt64(((Decimal) v).toJavaBigDecimal().longValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newInt64(((BigDecimal) v).longValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt64(((Number) v).longValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newInt64(((UTF8String) v).toLongExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertInt32ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newInt32(((Decimal) v).toJavaBigDecimal().intValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newInt32(((BigDecimal) v).intValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt32(((Number) v).intValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newInt32(((UTF8String) v).toIntExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertInt16ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newInt16(((Decimal) v).toJavaBigDecimal().shortValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newInt16(((BigDecimal) v).shortValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt16(((Number) v).shortValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newInt16(((UTF8String) v).toShortExact());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertFloatToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newFloat(((Decimal) v).toJavaBigDecimal().floatValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newFloat(((BigDecimal) v).floatValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newFloat(((Number) v).floatValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newFloat(Float.parseFloat(((UTF8String) v).toString()));
        }
        throw badConversion(v, t);
    }

    private Value<?> convertDoubleToYdb(Object v, PrimitiveType t) {
        if (v instanceof Decimal) {
            return PrimitiveValue.newDouble(((Decimal) v).toJavaBigDecimal().doubleValue());
        }
        if (v instanceof BigDecimal) {
            return PrimitiveValue.newDouble(((BigDecimal) v).doubleValue());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newDouble(((Number) v).doubleValue());
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newDouble(Double.parseDouble(((UTF8String) v).toString()));
        }
        throw badConversion(v, t);
    }

    private Value<?> convertDecimalToYdb(Object v, DecimalType t) {
        if (v instanceof Decimal) {
            return t.newValue(((Decimal) v).toJavaBigDecimal());
        }
        if (v instanceof BigDecimal) {
            return t.newValue(((BigDecimal) v));
        }
        if (v instanceof Double) {
            return t.newValue(new BigDecimal((Double) v));
        }
        if (v instanceof Float) {
            return t.newValue(new BigDecimal((Float) v));
        }
        if (v instanceof Number) {
            return t.newValue(new BigDecimal(((Number) v).longValue()));
        }
        if (v instanceof UTF8String) {
            return t.newValue(((UTF8String) v).toString());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertTimestampToYdb(Object v, PrimitiveType t) {
        if (v instanceof Long) {
            return PrimitiveValue.newTimestamp(DateTimeUtils.microsToInstant((Long) v));
        }
        try {
            if (v instanceof String || v instanceof UTF8String) {
                return PrimitiveValue.newTimestamp(Instant.parse(v.toString()));
            }
        } catch (DateTimeParseException dtpe) {
        }
        throw badConversion(v, t);
    }

    private Value<?> convertDatetimeToYdb(Object v, PrimitiveType t) {
        if (v instanceof java.sql.Timestamp) {
            return PrimitiveValue.newDatetime(((java.sql.Timestamp) v).toInstant());
        }
        if (v instanceof Long) {
            return PrimitiveValue.newDatetime(DateTimeUtils.microsToInstant((Long) v));
        }
        try {
            if (v instanceof String || v instanceof UTF8String) {
                return PrimitiveValue.newDatetime(LocalDateTime.parse(v.toString()));
            }
        } catch (DateTimeParseException dtpe) {
        }
        throw badConversion(v, t);
    }

    private Value<?> convertDateToYdb(Object v, PrimitiveType t) {
        if (v instanceof java.sql.Date) {
            return PrimitiveValue.newDate(((java.sql.Date) v).toLocalDate());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newDate(LocalDate.ofEpochDay(((Number) v).longValue()));
        }
        try {
            if (v instanceof String || v instanceof UTF8String) {
                return PrimitiveValue.newDate(LocalDate.parse(v.toString()));
            }
        } catch (DateTimeParseException dtpe) {
        }
        throw badConversion(v, t);
    }

    private Value<?> convertTimestamp64ToYdb(Object v, PrimitiveType t) {
        if (v instanceof Long) {
            return PrimitiveValue.newTimestamp64(DateTimeUtils.microsToInstant((Long) v));
        }
        try {
            if (v instanceof String || v instanceof UTF8String) {
                return PrimitiveValue.newTimestamp64(Instant.parse(v.toString()));
            }
        } catch (DateTimeParseException dtpe) {
        }
        throw badConversion(v, t);
    }

    private Value<?> convertDatetime64ToYdb(Object v, PrimitiveType t) {
        if (v instanceof java.sql.Timestamp) {
            return PrimitiveValue.newDatetime64(((java.sql.Timestamp) v).toInstant());
        }
        if (v instanceof Long) {
            return PrimitiveValue.newDatetime64(DateTimeUtils.microsToInstant((Long) v));
        }
        try {
            if (v instanceof String || v instanceof UTF8String) {
                return PrimitiveValue.newDatetime64(LocalDateTime.parse(v.toString()));
            }
        } catch (DateTimeParseException dtpe) {
        }
        throw badConversion(v, t);
    }

    private Value<?> convertDate32ToYdb(Object v, PrimitiveType t) {
        if (v instanceof java.sql.Date) {
            return PrimitiveValue.newDate32(((java.sql.Date) v).toLocalDate());
        }
        if (v instanceof Number) {
            return PrimitiveValue.newDate32(LocalDate.ofEpochDay(((Number) v).longValue()));
        }
        try {
            if (v instanceof String || v instanceof UTF8String) {
                return PrimitiveValue.newDate32(LocalDate.parse(v.toString()));
            }
        } catch (DateTimeParseException dtpe) {
        }
        throw badConversion(v, t);
    }

    private Value<?> convertBytesToYdb(Object v, PrimitiveType t) {
        if (v instanceof Bytes) {
            return PrimitiveValue.newBytes(((Bytes) v).getData());
        }
        if (v instanceof byte[]) {
            return PrimitiveValue.newBytes((byte[]) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newBytes(v.toString().getBytes(StandardCharsets.UTF_8));
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newBytes(((UTF8String) v).getBytes());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertBoolToYdb(Object v, PrimitiveType t) {
        if (v instanceof Boolean) {
            return PrimitiveValue.newBool((Boolean) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newBool(((Number) v).intValue() != 0);
        }
        if (v instanceof UTF8String) {
            return PrimitiveValue.newBool(Boolean.parseBoolean(((UTF8String) v).toString()));
        }
        throw badConversion(v, t);
    }

    private static UnsupportedOperationException badConversion(Object v, Type t) {
        throw new UnsupportedOperationException("Cannot convert value [" + v + "] of class "
                + v.getClass().getName() + " to type " + t);
    }

    @SuppressWarnings("unchecked")
    public static Serializable max(Serializable o1, Serializable o2) {
        if (o1 == null || o1 == o2) {
            return o2;
        }
        if (o2 == null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable) o2).compareTo(o1) > 0 ? o2 : o1;
        }
        return o2;
    }

    @SuppressWarnings("unchecked")
    public static Serializable min(Serializable o1, Serializable o2) {
        if (o1 == null || o1 == o2) {
            return o2;
        }
        if (o2 == null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable) o2).compareTo(o1) < 0 ? o2 : o1;
        }
        return o2;
    }

    public void setRowValue(InternalRow row, int i, ValueReader vr) {
        Type type = vr.getType();
        if (type.getKind() == Type.Kind.OPTIONAL) {
            if (!vr.isOptionalItemPresent()) {
                row.setNullAt(i);
                return;
            }
            type = type.unwrapOptional();
        }

        if (type.getKind() == Type.Kind.DECIMAL) {
            DecimalType decimal = (DecimalType) type;
            Decimal.apply(vr.getDecimal().toBigDecimal());
            row.setDecimal(i, Decimal.apply(vr.getDecimal().toBigDecimal()), decimal.getPrecision());
            return;
        }

        if (type.getKind() == Type.Kind.PRIMITIVE) {
            PrimitiveType primitiveType = (PrimitiveType) type;
            switch (primitiveType) {
                case Bool:
                    row.setBoolean(i, vr.getBool());
                    break;
                case Int8:
                    row.setByte(i, vr.getInt8());
                    break;
                case Int16:
                    row.setShort(i, vr.getInt16());
                    break;
                case Int32:
                    row.setInt(i, vr.getInt32());
                    break;
                case Int64:
                    row.setLong(i, vr.getInt64());
                    break;
                case Uint8:
                    row.setShort(i, (short) vr.getUint8());
                    break;
                case Uint16:
                    row.setInt(i, vr.getUint16());
                    break;
                case Uint32:
                    row.setLong(i, vr.getUint32());
                    break;
                case Uint64:
                    row.update(i, Decimal.apply(vr.getUint64()));
                    break;
                case Float:
                    row.setFloat(i, vr.getFloat());
                    break;
                case Double:
                    row.setDouble(i, vr.getDouble());
                    break;
                case Bytes:
                    row.update(i, vr.getBytes());
                    break;
                case Text:
                    row.update(i, UTF8String.fromString(vr.getText()));
                    break;
                case Yson:
                    row.update(i, vr.getYson());
                    break;
                case Json:
                    row.update(i, UTF8String.fromString(vr.getJson()));
                    break;
                case Uuid:
                    row.update(i, vr.getUuid());
                    break;
                case JsonDocument:
                    row.update(i, UTF8String.fromString(vr.getJsonDocument()));
                    break;
                case Date:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getDate().toString()));
                    } else {
                        row.setInt(i, (int) vr.getDate().toEpochDay());
                    }
                    break;
                case Datetime:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getDatetime().toString()));
                    } else {
                        row.setLong(i, vr.getDatetime().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L);
                    }
                    break;
                case Timestamp:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getTimestamp().toString()));
                    } else {
                        row.setLong(i, vr.getTimestamp().toEpochMilli() * 1000L);
                    }
                    break;
                case Interval:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getInterval().toString()));
                    } else {
                        row.update(i, vr.getInterval());
                    }
                    break;
                case Date32:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getDate32().toString()));
                    } else {
                        row.setInt(i, (int) vr.getDate32().toEpochDay());
                    }
                    break;
                case Datetime64:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getDatetime64().toString()));
                    } else {
                        row.setLong(i, vr.getDatetime64().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L);
                    }
                    break;
                case Timestamp64:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getTimestamp64().toString()));
                    } else {
                        row.setLong(i, vr.getTimestamp64().toEpochMilli() * 1000L);
                    }
                    break;
                case Interval64:
                    if (dateAsString) {
                        row.update(i, UTF8String.fromString(vr.getInterval64().toString()));
                    } else {
                        row.update(i, vr.getInterval64());
                    }
                    break;
                case TzDate:
                case TzDatetime:
                case TzTimestamp:
                case DyNumber:
                default:
                    throw new IllegalArgumentException("Conversion from type " + primitiveType + " is not supported");
            }
            return;
        }

        throw new IllegalArgumentException("Conversion from type " + type + " is not supported");
    }

    public static class Bytes implements Comparable<Bytes>, Serializable {
        private static final long serialVersionUID = 20250904001L;

        private final byte[] data;

        public Bytes(byte[] data) {
            this.data = data;
        }

        public byte[] getData() {
            return data;
        }

        @Override
        public int compareTo(Bytes other) {
            return UnsignedBytes.lexicographicalComparator()
                    .compare(this.data, other.data);
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + Arrays.hashCode(this.data);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Bytes other = (Bytes) obj;
            return Arrays.equals(this.data, other.data);
        }

        @Override
        public String toString() {
            if (data == null) {
                return "<null>";
            }
            return Base64.getEncoder().encodeToString(data);
        }
    }
}
