package tech.ydb.spark.connector;

import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.SparkSessionExtensionsProvider;
import scala.runtime.BoxedUnit;

/**
 *
 * @author zinal
 */
public class YdbExtensions implements SparkSessionExtensionsProvider {

    @Override
    public BoxedUnit apply(SparkSessionExtensions ext) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
