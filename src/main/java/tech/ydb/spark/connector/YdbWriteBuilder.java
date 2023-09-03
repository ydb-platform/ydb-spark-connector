package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 *
 * @author zinal
 */
public class YdbWriteBuilder implements WriteBuilder {

    @Override
    public Write build() {
        return WriteBuilder.super.build(); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

}
