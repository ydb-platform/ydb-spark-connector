package tech.ydb.spark.connector.read;

import org.apache.spark.sql.connector.read.InputPartition;

import tech.ydb.spark.connector.common.YdbKeyRange;

/**
 * YDB scan partition is defined by the key range.
 *
 * TODO: improve with HasPartitionKey
 *
 * @author zinal
 */
public class YdbTablePartition implements InputPartition {

    private static final long serialVersionUID = -529216835125843329L;

    private final int orderingKey;
    private final YdbKeyRange range;

    public YdbTablePartition(int orderingKey, YdbKeyRange range) {
        this.orderingKey = orderingKey;
        this.range = range;
    }

    public int getOrderingKey() {
        return orderingKey;
    }

    public YdbKeyRange getRange() {
        return range;
    }

    @Override
    public String toString() {
        return (range == null) ? "range:unconfined" : ("range:" + range.toString());
    }
}
