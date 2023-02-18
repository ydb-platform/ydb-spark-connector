package tech.ydb.spark.connector;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import tech.ydb.table.values.PrimitiveType;

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
}

