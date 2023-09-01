package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 *
 * @author mzinal
 */
public class YdbInputPartition implements InputPartition {

    private final YdbKeyRange range;

    public YdbInputPartition() {
        this.range = null;
    }

    public YdbInputPartition(YdbKeyRange range) {
        this.range = range;
    }

    public YdbKeyRange getRange() {
        return range;
    }

    @Override
    public String toString() {
        return (range == null) ? "range-unconfined" : range.toString();
    }
}
