package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * YDB scan partition is defined by the key range.
 *
 * TODO: improve with HasPartitionKey
 *
 * @author zinal
 */
public class YdbTablePartition implements InputPartition {

    private static final long serialVersionUID = 1L;

    private final YdbKeyRange range;

    public YdbTablePartition(YdbKeyRange range) {
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
