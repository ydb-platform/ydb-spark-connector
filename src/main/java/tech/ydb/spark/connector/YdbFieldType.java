package tech.ydb.spark.connector;

import tech.ydb.table.values.Type;
import tech.ydb.table.values.PrimitiveType;

/**
 * Supported YDB data types for the columns.
 * Had to duplicate the type definitions here to simplify the type processing logic.
 *
 * @author zinal
 */
public enum YdbFieldType {
    Bool,
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float,
    Double,
    Bytes,
    Text,
    Yson,
    Json,
    Uuid,
    Date,
    Datetime,
    Timestamp,
    Interval,
    TzDate,
    TzDatetime,
    TzTimestamp,
    JsonDocument,
    DyNumber,
    Decimal;

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

}
