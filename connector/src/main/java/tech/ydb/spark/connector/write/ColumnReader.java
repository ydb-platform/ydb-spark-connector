package tech.ydb.spark.connector.write;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ColumnReader implements ValueReader {
    private final int colIdx;
    private final DataType sparkType;
    private final Type ydbType;

    public ColumnReader(int colIdx, DataType sparkType, Type ydbType) {
        this.colIdx = colIdx;
        this.sparkType = sparkType;
        this.ydbType = ydbType;
    }

    @Override
    public Value<?> read(YdbTypes types, InternalRow row) {
        Object v = row.get(colIdx, sparkType);
        return types.convertToYdb(v, ydbType);
    }
}
