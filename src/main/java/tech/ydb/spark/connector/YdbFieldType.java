package tech.ydb.spark.connector;

import tech.ydb.table.values.Type;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.DecimalType;

/**
 * Supported YDB data types for the columns.
 * Had to duplicate the type definitions here to simplify the type processing logic.
 *
 * @author zinal
 */
public enum YdbFieldType {
    Bool("Bool"),
    Int8("Int8"),
    Uint8("Uint8"),
    Int16("Int16"),
    Uint16("Uint16"),
    Int32("Int32"),
    Uint32("Uint32"),
    Int64("Int64"),
    Uint64("Uint64"),
    Float("Float"),
    Double("Double"),
    Bytes("String"),
    Text("Utf8"),
    Yson("Yson"),
    Json("Json"),
    JsonDocument("JsonDocument"),
    Uuid("Uuid"),
    Date("Date"),
    Datetime("Datetime"),
    Timestamp("Timestamp"),
    Interval("Interval"),
    TzDate("TzDate"),
    TzDatetime("TzDatetime"),
    TzTimestamp("TzTimestamp"),
    DyNumber("DyNumber"),
    Decimal("Decimal(22,9)");

    public final String sqlName;

    YdbFieldType(String sqlName) {
        this.sqlName = sqlName;
    }

    public Type toSdkType() {
        return toSdkType(this, false);
    }

    public Type toSdkType(boolean optional) {
        return toSdkType(this, optional);
    }

    public static YdbFieldType fromSdkType(Type t) {
        switch (t.getKind()) {
            case OPTIONAL:
                t = t.unwrapOptional();
                break;
        }
        switch (t.getKind()) {
            case DECIMAL:
                return Decimal;
            case PRIMITIVE:
                switch (((PrimitiveType)t)) {
                    case Bool:
                        return Bool;
                    case Int8:
                        return Int8;
                    case Uint8:
                        return Uint8;
                    case Int16:
                        return Int16;
                    case Uint16:
                        return Uint16;
                    case Int32:
                        return Int32;
                    case Uint32:
                        return Uint32;
                    case Int64:
                        return Int64;
                    case Uint64:
                        return Uint64;
                    case Float:
                        return Float;
                    case Double:
                        return Double;
                    case Bytes:
                        return Bytes;
                    case Text:
                        return Text;
                    case Yson:
                        return Yson;
                    case Json:
                        return Json;
                    case Uuid:
                        return Uuid;
                    case Date:
                        return Date;
                    case Datetime:
                        return Datetime;
                    case Timestamp:
                        return Timestamp;
                    case Interval:
                        return Interval;
                    case TzDate:
                        return TzDate;
                    case TzDatetime:
                        return TzDatetime;
                    case TzTimestamp:
                        return TzTimestamp;
                    case JsonDocument:
                        return JsonDocument;
                    case DyNumber:
                        return DyNumber;
                }
        }
        throw new IllegalArgumentException(t.toString());
    }

    public static Type toSdkType(YdbFieldType ft, boolean optional) {
        Type t;
        switch (ft) {
            case Bool: t = PrimitiveType.Bool; break;
            case Int8: t = PrimitiveType.Int8; break;
            case Uint8: t = PrimitiveType.Uint8; break;
            case Int16: t = PrimitiveType.Int16; break;
            case Uint16: t = PrimitiveType.Uint16; break;
            case Int32: t = PrimitiveType.Int32; break;
            case Uint32: t = PrimitiveType.Uint32; break;
            case Int64: t = PrimitiveType.Int64; break;
            case Uint64: t = PrimitiveType.Uint64; break;
            case Float: t = PrimitiveType.Float; break;
            case Double: t = PrimitiveType.Double; break;
            case Bytes: t = PrimitiveType.Bytes; break;
            case Text: t =  PrimitiveType.Text; break;
            case Yson: t = PrimitiveType.Yson; break;
            case Json: t = PrimitiveType.Json; break;
            case JsonDocument: t = PrimitiveType.JsonDocument; break;
            case Uuid: t = PrimitiveType.Uuid; break;
            case Date: t = PrimitiveType.Date; break;
            case Datetime: t = PrimitiveType.Datetime; break;
            case Timestamp: t = PrimitiveType.Timestamp; break;
            case Interval: t = PrimitiveType.Interval; break;
            case TzDate: t = PrimitiveType.TzDate; break;
            case TzDatetime: t = PrimitiveType.TzDatetime; break;
            case TzTimestamp: t = PrimitiveType.TzTimestamp; break;
            case DyNumber: t = PrimitiveType.DyNumber; break;
            case Decimal: t = DecimalType.getDefault(); break;
            default: throw new IllegalArgumentException("Illegal input type " + ft);
        }
        return optional ? t.makeOptional() : t;
    }

}
