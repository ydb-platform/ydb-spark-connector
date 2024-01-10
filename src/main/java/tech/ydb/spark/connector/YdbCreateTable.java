package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import tech.ydb.core.Status;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.PartitioningSettings;

/**
 * Create table implementation.
 *
 * @author zinal
 */
class YdbCreateTable extends YdbPropertyHelper {

    private final String tablePath;
    private final List<YdbFieldInfo> fields;
    private final List<String> primaryKey;

    YdbCreateTable(String tablePath, List<YdbFieldInfo> fields,
            List<String> primaryKey, Map<String, String> properties) {
        super(properties);
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = primaryKey;
    }

    YdbCreateTable(String tablePath, List<YdbFieldInfo> fields,
            Map<String, String> properties) {
        super(properties);
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = makePrimaryKey(fields, properties);
    }

    CompletableFuture<Status> createTable(Session session) {
        TableDescription.Builder tdb = TableDescription.newBuilder();
        for (YdbFieldInfo yfi : fields) {
            if (yfi.isNullable()) {
                tdb.addNullableColumn(yfi.getName(), yfi.getType().toSdkType());
            } else {
                tdb.addNonnullColumn(yfi.getName(), yfi.getType().toSdkType());
            }
        }
        tdb.setPrimaryKeys(primaryKey);

        final PartitioningSettings ps = new PartitioningSettings();
        ps.setPartitioningBySize(getBooleanOption(YdbOptions.AP_BY_SIZE, true));
        ps.setPartitioningByLoad(getBooleanOption(YdbOptions.AP_BY_LOAD, true));
        long minPartitions = getLongOption(YdbOptions.AP_MIN_PARTS, 1L);
        if (minPartitions < 1) {
            minPartitions = 1L;
        }
        long maxPartitions = getLongOption(YdbOptions.AP_MAX_PARTS, 50L);
        if (maxPartitions < minPartitions) {
            maxPartitions = minPartitions + 49L + (minPartitions / 100L);
        }
        ps.setMinPartitionsCount(minPartitions);
        ps.setMaxPartitionsCount(maxPartitions);
        long minSizeMb = getLongOption(YdbOptions.AP_PART_SIZE_MB, 2000L);
        if (minSizeMb < 1L) {
            minSizeMb = 10L;
        }
        ps.setPartitionSize(minSizeMb);
        tdb.setPartitioningSettings(ps);

        return session.createTable(tablePath, tdb.build());
    }

    static List<YdbFieldInfo> convert(YdbTypes types, StructType st) {
        final List<YdbFieldInfo> fields = new ArrayList<>(st.size());
        for (StructField sf : JavaConverters.asJavaCollection(st)) {
            YdbFieldType yft = types.mapTypeSpark2Ydb(sf.dataType());
            if (yft == null) {
                throw new IllegalArgumentException("Unsupported type for table column: "
                        + sf.dataType());
            }
            fields.add(new YdbFieldInfo(sf.name(), yft, sf.nullable()));
        }
        return fields;
    }

    static List<String> makePrimaryKey(List<YdbFieldInfo> fields, Map<String, String> properties) {
        String value = properties.get(YdbOptions.PRIMARY_KEY);
        if (value == null) {
            value = fields.get(0).getName();
        }
        return Arrays.asList(value.split("[,]"));
    }

}
