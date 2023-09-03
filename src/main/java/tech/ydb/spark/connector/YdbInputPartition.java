package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * YDB scan partition is defined by the key range.
 *
 * @author zinal
 */
public class YdbInputPartition implements InputPartition {

    private final YdbKeyRange range;

    public YdbInputPartition(YdbKeyRange range) {
        this.range = range;
    }

    public YdbKeyRange getRange() {
        return range;
    }

    @Override
    public String toString() {
        return (range == null) ? "range:unconfined" : ("range:" + range.toString());
    }
}
