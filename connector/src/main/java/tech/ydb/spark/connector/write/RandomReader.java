package tech.ydb.spark.connector.write;

import java.util.UUID;

import org.apache.spark.sql.catalyst.InternalRow;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class RandomReader implements ValueReader {
    @Override
    public Value<?> read(YdbTypes types, InternalRow row) {
        return PrimitiveValue.newText(UUID.randomUUID().toString());
    }
}
