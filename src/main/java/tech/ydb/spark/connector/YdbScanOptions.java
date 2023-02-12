package tech.ydb.spark.connector;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import tech.ydb.table.description.TableDescription;

/**
 *
 * @author zinal
 */
public class YdbScanOptions {

    private final TableDescription td;

    public YdbScanOptions(TableDescription td) {
        this.td = td;
    }

    public YdbScanOptions(YdbScanOptions src) {
        this.td = src.td; // we expect table description to be non-mutable
    }

    public Filter[] pushFilters(Filter[] filters) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    public Filter[] pushedFilters() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    public void pruneColumns(StructType requiredSchema) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
