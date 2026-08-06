package tech.ydb.spark.connector.write;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;

import tech.ydb.core.Status;
import tech.ydb.table.Session;
import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 * Encodes Spark rows as Apache Arrow IPC payloads and ships them via {@code BulkUpsert}.
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public class YdbWriterArrow implements YdbWriter {
    private final List<ColumnEntry> columns;
    private final String tablePath;
    private final ApacheArrowWriter arrowWriter;
    private final BulkUpsertSettings settings = new BulkUpsertSettings();
    private final int maxRowsCount;
    private final int maxBytesSize;

    private ApacheArrowWriter.Batch batch;
    private int rowsCount = 0;
    private int bytesSize = 0;

    public YdbWriterArrow(String tablePath, List<ColumnEntry> columns, int maxRowsCount, int maxBytesSize) {
        this.columns = columns;
        this.tablePath = tablePath;
        this.maxRowsCount = maxRowsCount;
        this.maxBytesSize = maxBytesSize;

        ApacheArrowWriter.Schema schema = ApacheArrowWriter.newSchema();
        for (ColumnEntry column: columns) {
            schema.addColumn(column.getName(), column.getType());
        }

        this.arrowWriter = schema.createWriter(ArrowUtils.rootAllocator());
        this.batch = arrowWriter.createNewBatch(0);
    }

    @Override
    public String toString() {
        return "YdbWriterArrow[" + tablePath + "]";
    }

    @Override
    public void appendRow(InternalRow record) {
        ApacheArrowWriter.Row row = batch.writeNextRow();
        for (ColumnEntry column: columns) {
            bytesSize += writeValue(column, row, record);
        }
        rowsCount++;
    }

    @Override
    public boolean needToFlush() {
        return bytesSize >= maxBytesSize || rowsCount >= maxRowsCount;
    }

    @Override
    public void close() {
        arrowWriter.close();
    }

    @Override
    public Batch buildNextBatch() {
        if (rowsCount <= 0) {
            return null;
        }
        try {
            Batch next = new Batch() {
                private final int count = rowsCount;
                private final ApacheArrowData data = batch.buildBatch();
                @Override
                public int rowsCount() {
                    return count;
                }

                @Override
                public int bytesSize() {
                    return data.getData().size() + data.getSchema().size();
                }

                @Override
                public CompletableFuture<Status> apply(Session session) {
                    return session.executeBulkUpsert(tablePath, data, settings);
                }
            };

            rowsCount = 0;
            bytesSize = 0;
            batch = arrowWriter.createNewBatch(0);
            return next;
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to serialize Arrow batch", ex);
        }
    }

    private static String castMsg(DataType dataType, Type type) {
        return "Arrow ingestion does not support casting " + dataType + " to " + type;
    }

    private static byte[] readBytes(InternalRow record, DataType dataType, Type type, int ordinal) {
        if (!dataType.sameType(DataTypes.BinaryType)) {
            throw new IllegalArgumentException(castMsg(dataType, type));
        }
        return (byte[]) record.get(ordinal, dataType);
    }

    @SuppressWarnings("MethodLength")
    private static int writeValue(ColumnEntry column, ApacheArrowWriter.Row row, InternalRow record) {
        DataType dataType = column.getDataType();
        String name = column.getName();
        int ordinal = column.getOrdinal();
        Type type = column.getType();

        if (dataType == null) {
            String randomKey = UUID.randomUUID().toString();
            row.writeText(column.getName(), randomKey);
            return randomKey.length();
        }

        while (type.getKind() == Type.Kind.OPTIONAL) {
            if (record.isNullAt(ordinal)) {
                row.writeNull(name);
                return column.getDataType().defaultSize();
            }
            type = type.unwrapOptional();
        }

        if (type.getKind() == Type.Kind.DECIMAL) {
            DecimalType realType = (DecimalType) type;
            Decimal v = record.getDecimal(ordinal, realType.getPrecision(), realType.getScale());
            row.writeDecimal(name, realType.newValue(v.toJavaBigDecimal()));
            // YDB stores Decimal as array of 16 bytes
            return 16;
        }

        if (type.getKind() != Type.Kind.PRIMITIVE) {
            throw new IllegalArgumentException("Arrow ingestion does not support type " + type);
        }

        switch ((PrimitiveType) type) {
            case Bool:
                row.writeBool(name, record.getBoolean(ordinal));
                return 1;
            case Int8:
                row.writeInt8(name, record.getByte(ordinal));
                return 1;
            case Int16:
                row.writeInt16(name, record.getShort(ordinal));
                return 2;
            case Int32:
                row.writeInt32(name, record.getInt(ordinal));
                return 4;
            case Int64:
                row.writeInt64(name, record.getLong(ordinal));
                return 8;
            case Uint8:
                row.writeUint8(name, record.getInt(ordinal));
                return 1;
            case Uint16:
                row.writeUint16(name, record.getInt(ordinal));
                return 2;
            case Uint32:
                row.writeUint32(name, record.getLong(ordinal));
                return 4;
            case Uint64:
                Decimal uint64 = record.getDecimal(ordinal, 22, 0);
                row.writeUint64(name, uint64.toJavaBigInteger().longValue());
                return 8;
            case Float:
                row.writeFloat(name, record.getFloat(ordinal));
                return 4;
            case Double:
                row.writeDouble(name, record.getDouble(ordinal));
                return 8;
            case Text:
                UTF8String text = record.getUTF8String(ordinal);
                row.writeText(name, text.toString());
                return text.numBytes();
            case Json:
                UTF8String json = record.getUTF8String(ordinal);
                row.writeJson(name, json.toString());
                return json.numBytes();
            case JsonDocument:
                UTF8String jsonDocument = record.getUTF8String(ordinal);
                row.writeJsonDocument(name, jsonDocument.toString());
                return jsonDocument.numBytes();
            case Bytes:
                byte[] bytes = readBytes(record, dataType, type, ordinal);
                row.writeBytes(name, bytes);
                return bytes.length;
            case Yson:
                byte[] yson = readBytes(record, dataType, type, ordinal);
                row.writeYson(name, yson);
                return yson.length;
            case Uuid:
                UTF8String uuid = record.getUTF8String(ordinal);
                row.writeUuid(name, UUID.fromString(uuid.toString()));
                return 16;
            case Date:
                if (dataType.sameType(DataTypes.DateType)) {
                    row.writeDate(name, LocalDate.ofEpochDay(record.getInt(ordinal)));
                } else {
                    throw new IllegalArgumentException(castMsg(dataType, type));
                }
                return 2;
            case Datetime:
                if (dataType.sameType(DataTypes.TimestampType)) {
                    row.writeDatetime(name, DateTimeUtils.microsToLocalDateTime(record.getLong(ordinal)));
                } else {
                    throw new IllegalArgumentException(castMsg(dataType, type));
                }
                return 4;
            case Timestamp:
                if (dataType.sameType(DataTypes.TimestampType)) {
                    row.writeTimestamp(name, DateTimeUtils.microsToInstant(record.getLong(ordinal)));
                } else {
                    throw new IllegalArgumentException(castMsg(dataType, type));
                }
                return 8;
//            case Interval:
//                if (dataType.sameType(DataTypes.CalendarIntervalType)) {
//                    row.writeInterval(name, Duration.ofMillis(record.getLong(ordinal)));
//                } else {
//                    throw new IllegalArgumentException(castMsg(dataType, type));
//                }
//                return 8;
            case Date32:
                if (dataType.sameType(DataTypes.DateType)) {
                    row.writeDate32(name, LocalDate.ofEpochDay(record.getInt(ordinal)));
                } else {
                    throw new IllegalArgumentException(castMsg(dataType, type));
                }
                return 4;
            case Datetime64:
                if (dataType.sameType(DataTypes.TimestampType)) {
                    row.writeDatetime64(name, DateTimeUtils.microsToLocalDateTime(record.getLong(ordinal)));
                } else {
                    throw new IllegalArgumentException(castMsg(dataType, type));
                }
                return 8;
            case Timestamp64:
                if (dataType.sameType(DataTypes.TimestampType)) {
                    row.writeTimestamp64(name, DateTimeUtils.microsToInstant(record.getLong(ordinal)));
                } else {
                    throw new IllegalArgumentException(castMsg(dataType, type));
                }
                return 8;
//            case Interval64:
//                if (dataType.sameType(DataTypes.CalendarIntervalType)) {
//                    row.writeInterval64(name, Duration.ofMillis(record.getLong(ordinal)));
//                } else {
//                    throw new IllegalArgumentException(castMsg(dataType, type));
//                }
//                return 8;
            default:
                throw new IllegalArgumentException("Arrow ingestion does not support type " + type);
        }
    }
}
