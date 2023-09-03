package tech.ydb.spark.connector;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *
 * @author zinal
 */
public class YdbProvider implements TableProvider {

    private YdbTable table;

    /*
    private YdbTable getOrLoadTable(Map<String, String> properties) {
        
        YdbConnector connector = YdbRegistry.create(properties);
        return
    }
    */

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return TableProvider.super.inferPartitioning(options); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
