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
import java.util.List;

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
import tech.ydb.spark.connector.common.FieldType;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.description.TableColumn;
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
public final class YdbTypes implements Serializable {
    private static final long serialVersionUID = 7729504281996858720L;

    public static final DataType SPARK_DECIMAL = DataTypes.createDecimalType(38, 10);
    public static final DataType SPARK_UINT64 = DataTypes.createDecimalType(22, 0);

    private final boolean dateAsString;

    public YdbTypes(CaseInsensitiveStringMap options) {
        this.dateAsString = OperationOption.DATE_AS_STRING.readBoolean(options, false);
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
        FieldType type = mapTypeSpark2Ydb(field.dataType());
        if (type == null) {
            throw new IllegalArgumentException("Unsupported type for table column: " + field.dataType());
        }
        return new FieldInfo(field.name(), type, field.nullable());
    }

    public List<FieldInfo> fromSparkSchema(StructType schema) {
        final List<FieldInfo> fields = new ArrayList<>(schema.size());
        for (StructField field : schema.fields()) {
            FieldType type = mapTypeSpark2Ydb(field.dataType());
            if (type == null) {
                throw new IllegalArgumentException("Unsupported type for table column: " + field.dataType());
            }
            fields.add(new FieldInfo(field.name(), type, field.nullable()));
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

    private DataType mapTypeYdb2Spark(tech.ydb.table.values.Type yt) {
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
            case DECIMAL:
                return SPARK_DECIMAL;
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
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.DateType;
            case Datetime:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.TimestampType;
            case Timestamp:
                if (dateAsString) {
                    return DataTypes.StringType;
                }
                return DataTypes.TimestampType;
            case Interval:
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
                return DataTypes.createDecimalType(38, 10);
            default: {
                /* noop */
            }
        }
        return null;
    }

    public FieldType mapTypeSpark2Ydb(org.apache.spark.sql.types.DataType type) {
        if (type instanceof org.apache.spark.sql.types.DecimalType) {
            org.apache.spark.sql.types.DecimalType x = (org.apache.spark.sql.types.DecimalType) type;
            if (x.scale() == 0) {
                if (x.precision() <= 21) {
                    return FieldType.Int64;
                }
            }
            if (x.scale() == 38 && x.precision() == 10) {
                return FieldType.DyNumber;
            }
            return FieldType.Decimal;
        }
        if (type instanceof org.apache.spark.sql.types.VarcharType) {
            return FieldType.Text;
        }
        if (DataTypes.BooleanType.sameType(type)) {
            return FieldType.Bool;
        }
        if (DataTypes.ByteType.sameType(type)) {
            return FieldType.Int8;
        }
        if (DataTypes.ShortType.sameType(type)) {
            return FieldType.Int16;
        }
        if (DataTypes.IntegerType.sameType(type)) {
            return FieldType.Int32;
        }
        if (DataTypes.LongType.sameType(type)) {
            return FieldType.Int64;
        }
        if (DataTypes.FloatType.sameType(type)) {
            return FieldType.Float;
        }
        if (DataTypes.DoubleType.sameType(type)) {
            return FieldType.Double;
        }
        if (DataTypes.BinaryType.sameType(type)) {
            return FieldType.Bytes;
        }
        if (DataTypes.StringType.sameType(type)) {
            return FieldType.Text;
        }
        if (DataTypes.DateType.sameType(type)) {
            return FieldType.Date;
        }
        if (DataTypes.TimestampType.sameType(type)) {
            return FieldType.Timestamp;
        }
        return null;
    }

    public Object convertFromYdb(ValueReader vr) {
        if (vr == null) {
            return null;
        }
        Type t = vr.getType();
        if (t.getKind().equals(Type.Kind.OPTIONAL)) {
            if (!vr.isOptionalItemPresent()) {
                return null;
            }
            t = t.unwrapOptional();
        }
        switch (t.getKind()) {
            case PRIMITIVE:
                switch ((PrimitiveType) t) {
                    case Bool:
                        return vr.getBool();
                    case Bytes:
                        return vr.getBytes();
                    case Date:
                        if (dateAsString) {
                            return UTF8String.fromString(vr.getDate().toString());
                        }
                        return (int) vr.getDate().toEpochDay();
                    case Datetime:
                        if (dateAsString) {
                            return UTF8String.fromString(vr.getDatetime().toString());
                        }
                        return vr.getDatetime().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
                    case Timestamp:
                        if (dateAsString) {
                            return UTF8String.fromString(vr.getTimestamp().toString());
                        }
                        return vr.getTimestamp().toEpochMilli() * 1000L;
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
                    case Uint16:
                        return vr.getUint16();
                    case Uint32:
                        return vr.getUint32();
                    case Uint64: {
                        long v = vr.getUint64();
                        return Decimal.apply(new BigDecimal(Long.toUnsignedString(v)));
                    }
                    case Uuid:
                        return vr.getUuid();
                    case Yson:
                        return vr.getYson();
                    default: {
                        /* noop */
                    }
                }
                break;
            case DECIMAL:
                return Decimal.apply(vr.getDecimal().toBigDecimal());
            default: {
                /* noop */
            }
        }
        return null;
    }

    public Serializable convertFromYdb(Value<?> v) {
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
                        return v.asData().getBytes();
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

    public Value<?> convertToYdb(Object v, FieldType t) {
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
            case Decimal:
                return convertDecimalToYdb(v, t);
            case Double:
                return convertDoubleToYdb(v, t);
            case Float:
                return convertFloatToYdb(v, t);
            case Int16:
                return convertInt16ToYdb(v, t);
            case Int32:
                return convertInt32ToYdb(v, t);
            case Int64:
                return convertInt64ToYdb(v, t);
            case Int8:
                return convertInt8ToYdb(v, t);
            case Uint16:
                return convertUint16ToYdb(v, t);
            case Uint32:
                return convertUint32ToYdb(v, t);
            case Uint64:
                return convertUint64ToYdb(v, t);
            case Uint8:
                return convertUint8ToYdb(v, t);
            case Text:
                return convertTextToYdb(v);
            default:
                throw new IllegalArgumentException("Conversion to type " + t + " is not supported");
        }
    }

    private Value<?> convertTextToYdb(Object v) {
        if (v == null) {
            return PrimitiveType.Text.makeOptional().emptyValue();
        }
        return PrimitiveValue.newText(v.toString());
    }

    private Value<?> convertUint8ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Uint8.makeOptional().emptyValue();
        }
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

    private Value<?> convertUint64ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Uint64.makeOptional().emptyValue();
        }
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

    private Value<?> convertUint32ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Uint32.makeOptional().emptyValue();
        }
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

    private Value<?> convertUint16ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Uint16.makeOptional().emptyValue();
        }
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

    private Value<?> convertInt8ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Int8.makeOptional().emptyValue();
        }
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

    private Value<?> convertInt64ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Int64.makeOptional().emptyValue();
        }
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

    private Value<?> convertInt32ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Int32.makeOptional().emptyValue();
        }
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

    private Value<?> convertInt16ToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Int16.makeOptional().emptyValue();
        }
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

    private Value<?> convertFloatToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Float.makeOptional().emptyValue();
        }
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

    private Value<?> convertDoubleToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Double.makeOptional().emptyValue();
        }
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

    private Value<?> convertDecimalToYdb(Object v, FieldType t) {
        if (v == null) {
            return DecimalType.getDefault().makeOptional().emptyValue();
        }
        if (v instanceof Decimal) {
            return DecimalType.getDefault().newValue(((Decimal) v).toJavaBigDecimal());
        }
        if (v instanceof BigDecimal) {
            return DecimalType.getDefault().newValue(((BigDecimal) v));
        }
        if (v instanceof Double) {
            return DecimalType.getDefault().newValue(new BigDecimal((Double) v));
        }
        if (v instanceof Float) {
            return DecimalType.getDefault().newValue(new BigDecimal((Float) v));
        }
        if (v instanceof Number) {
            return DecimalType.getDefault().newValue(new BigDecimal(((Number) v).longValue()));
        }
        if (v instanceof UTF8String) {
            return DecimalType.getDefault().newValue(((UTF8String) v).toString());
        }
        throw badConversion(v, t);
    }

    private Value<?> convertTimestampToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Timestamp.makeOptional().emptyValue();
        }
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

    private Value<?> convertDatetimeToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Datetime.makeOptional().emptyValue();
        }
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

    private Value<?> convertDateToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Date.makeOptional().emptyValue();
        }
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

    private Value<?> convertBytesToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Bytes.makeOptional().emptyValue();
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

    private Value<?> convertBoolToYdb(Object v, FieldType t) {
        if (v == null) {
            return PrimitiveType.Bool.makeOptional().emptyValue();
        }
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

    private static UnsupportedOperationException badConversion(Object v, FieldType t) {
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

    public Value<?> getRowValue(InternalRow row, int i, Type type) {
        if (type.getKind() == Type.Kind.OPTIONAL) {
            type = type.unwrapOptional();

            if (row.isNullAt(i)) {
                return type.makeOptional().emptyValue();
            } else {
                return type.makeOptional().newValue(getRowValue(row, i, type));
            }
        }

        if (type.getKind() == Type.Kind.DECIMAL) {
            DecimalType decimalType = (DecimalType) type;
            Decimal value = row.getDecimal(i, decimalType.getPrecision(), decimalType.getScale());
            return decimalType.newValue(value.toJavaBigDecimal());
        }

        if (type.getKind() == Type.Kind.PRIMITIVE) {
            PrimitiveType primitiveType = (PrimitiveType) type;
            switch (primitiveType) {
                case Bool:
                    return PrimitiveValue.newBool(row.getBoolean(i));
                case Int8:
                    return PrimitiveValue.newInt8((byte) row.getInt(i));
                case Int16:
                    return PrimitiveValue.newInt16((short) row.getInt(i));
                case Int32:
                    return PrimitiveValue.newInt32(row.getInt(i));
                case Int64:
                    return PrimitiveValue.newInt64(row.getLong(i));
                case Uint8:
                    return PrimitiveValue.newUint8(row.getInt(i));
                case Uint16:
                    return PrimitiveValue.newUint16(row.getInt(i));
                case Uint32:
                    return PrimitiveValue.newUint32(row.getLong(i));
                case Uint64:
                    return PrimitiveValue.newUint64(row.getLong(i));
                case Float:
                    return PrimitiveValue.newFloat(row.getFloat(i));
                case Double:
                    return PrimitiveValue.newDouble(row.getDouble(i));
                case Bytes:
                    return PrimitiveValue.newBytes(row.getBinary(i));
                case Text:
                    return PrimitiveValue.newText(row.getUTF8String(i).toString());
                case Yson:
                    return PrimitiveValue.newYson(row.getBinary(i));
                case Json:
                    return PrimitiveValue.newJson(row.getUTF8String(i).toString());
                case Uuid:
                case JsonDocument:
                case Date:
                case Datetime:
                case Timestamp:
                case Interval:
                case TzDate:
                case TzDatetime:
                case TzTimestamp:
                case DyNumber:
                default:
                    throw new IllegalArgumentException("Conversion from type " + primitiveType + " is not supported");
            }
        }

        throw new IllegalArgumentException("Conversion from type " + type + " is not supported");
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
}
