package tech.ydb.spark.connector.read;

import tech.ydb.table.result.ValueReader;

/**
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public interface StreamPart extends AutoCloseable {
    int getRowCount();

    ValueReader getColumn(String name);

    boolean next();

    @Override
    void close();
}
