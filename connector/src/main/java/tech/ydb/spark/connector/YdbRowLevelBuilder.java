package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;

/**
 *
 * @author zinal
 */
public class YdbRowLevelBuilder implements RowLevelOperationBuilder {

    @Override
    public RowLevelOperation build() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
