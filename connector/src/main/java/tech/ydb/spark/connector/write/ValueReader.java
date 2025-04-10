package tech.ydb.spark.connector.write;

import org.apache.spark.sql.catalyst.InternalRow;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface ValueReader {
    Value<?> read(YdbTypes types, InternalRow row);
}
