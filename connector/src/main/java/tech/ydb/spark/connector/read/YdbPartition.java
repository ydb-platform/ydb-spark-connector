package tech.ydb.spark.connector.read;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbPartition extends InputPartition {
    SelectQuery makeQuery(SelectQuery origin);
}
