package tech.ydb.spark.connector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author mzinal
 */
public class YdbTable implements Table {

    static final Set<TableCapability> CAPABILITIES;
    static {
        final Set<TableCapability> c = new HashSet<>();
        c.add(TableCapability.BATCH_READ);
        c.add(TableCapability.MICRO_BATCH_READ);
        c.add(TableCapability.CONTINUOUS_READ);
        c.add(TableCapability.BATCH_WRITE);
        c.add(TableCapability.STREAMING_WRITE);
        c.add(TableCapability.OVERWRITE_BY_FILTER);
        c.add(TableCapability.V1_BATCH_WRITE);
        CAPABILITIES = Collections.unmodifiableSet(c);
    }

    @Override
    public String name() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public StructType schema() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Map<String, String> properties() {
        return Table.super.properties(); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

}
