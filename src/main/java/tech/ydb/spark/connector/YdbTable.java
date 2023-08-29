package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;

/**
 *
 * @author mzinal
 */
public class YdbTable implements Table, SupportsRead {

    static final Set<TableCapability> CAPABILITIES;
    static {
        final Set<TableCapability> c = new HashSet<>();
        c.add(TableCapability.BATCH_READ);
        CAPABILITIES = Collections.unmodifiableSet(c);
    }

    private final YdbConnector connector;
    private final String fullName;
    private final TableDescription td;
    private StructType schema;

    YdbTable(YdbConnector connector, String fullName, TableDescription td) {
        this.connector = connector;
        this.fullName = fullName;
        this.td = td;
    }

    final YdbConnector getConnector() {
        return connector;
    }

    final TableDescription getDescription() {
        return td;
    }

    @Override
    public String name() {
        return fullName;
    }

    @Override
    public StructType schema() {
        if (schema==null) {
            schema = new StructType(mapFields(td.getColumns()));
        }
        return schema;
    }

    @Override
    public Map<String, String> properties() {
        // TODO: return primary key + storage characteristics
        return Collections.emptyMap();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    private StructField[] mapFields(List<TableColumn> columns) {
        final List<StructField> fields = new ArrayList<>();
        for (TableColumn tc : columns) {
            final DataType dataType = YdbTypes.mapType(tc.getType());
            if (dataType != null)
                fields.add(mapField(tc, dataType));
        }
        return fields.toArray(new StructField[0]);
    }

    private StructField mapField(TableColumn tc, DataType dataType) {
        // TODO: mapping dictionary support (specifically for dates).
        return new StructField(tc.getName(), dataType, YdbTypes.mapNullable(tc.getType()), Metadata.empty());
    }

    @Override
    public Transform[] partitioning() {
        return new Transform[0];
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new YdbScanBuilder(this);
    }

    final List<String> keyColumns() {
        return new ArrayList<>(td.getPrimaryKeys());
    }

    final List<YdbFieldType> keyTypes() {
        Map<String,TableColumn> m = new HashMap<>();
        for (TableColumn tc : td.getColumns()) {
            m.put(tc.getName(), tc);
        }
        List<YdbFieldType> retval = new ArrayList<>();
        for (String kname : td.getPrimaryKeys()) {
            TableColumn tc = m.get(kname);
            retval.add(YdbFieldType.fromSdkType(tc.getType()));
        }
        return retval;
    }

    @Override
    public String toString() {
        return "YdbTable{" + "connector=" + connector + ", fullName=" + fullName + '}';
    }

}
