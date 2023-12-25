package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 *
 * @author zinal
 */
public class YdbWriteBuilder implements WriteBuilder {

    private final YdbTable table;
    private final LogicalWriteInfo info;

    public YdbWriteBuilder(YdbTable table, LogicalWriteInfo info) {
        this.table = table;
        this.info = info;
    }

    @Override
    public Write build() {
        return new YdbWrite(table, info);
    }

}
