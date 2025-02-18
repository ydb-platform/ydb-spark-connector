package tech.ydb.spark.connector.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import tech.ydb.core.Status;
import tech.ydb.spark.connector.YdbFieldInfo;
import tech.ydb.spark.connector.YdbFieldType;
import tech.ydb.spark.connector.YdbOptions;
import tech.ydb.spark.connector.YdbStoreType;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.PartitioningSettings;

/**
 * Create table implementation.
 *
 * @author zinal
 */
public class YdbCreateTable extends YdbPropertyHelper {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbCreateTable.class);

    private final String tablePath;
    private final List<YdbFieldInfo> fields;
    private final List<String> primaryKey;
    private final YdbStoreType storeType;

    public YdbCreateTable(String tablePath, List<YdbFieldInfo> fields,
            List<String> primaryKey, Map<String, String> properties) {
        super(properties);
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = primaryKey;
        this.storeType = getStoreType(properties);
    }

    public YdbCreateTable(String tablePath, List<YdbFieldInfo> fields,
            Map<String, String> properties) {
        super(properties);
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = makePrimaryKey(fields, properties);
        this.storeType = getStoreType(properties);
    }

    public CompletableFuture<Status> createTable(Session session) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating table {} with fields {} and PK {}",
                    tablePath, fields, primaryKey);
        }

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
        long minSizeMb = getLongOption(YdbOptions.AP_PART_SIZE_MB, 1000L);
        if (minSizeMb < 1L) {
            minSizeMb = 10L;
        }
        ps.setPartitionSize(minSizeMb);
        tdb.setPartitioningSettings(ps);

        /* TODO: implement store type configuration
        switch (storeType) {
            case ROW:
                tdb.setStoreType(TableDescription.StoreType.ROW);
                break;
            case COLUMN:
                tdb.setStoreType(TableDescription.StoreType.COLUMN);
                break;
            default:
                break;
        }
        */

        return session.createTable(tablePath, tdb.build());
    }

    public static List<YdbFieldInfo> convert(YdbTypes types, StructType st) {
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
            String autoPk = grabAutoPk(fields);
            return Arrays.asList(new String[]{autoPk});
        }
        return Arrays.asList(value.split("[,]"));
    }

    static String grabAutoPk(List<YdbFieldInfo> fields) {
        for (YdbFieldInfo yfi : fields) {
            if (YdbOptions.AUTO_PK.equalsIgnoreCase(yfi.getName())) {
                return yfi.getName();
            }
        }
        fields.add(new YdbFieldInfo(YdbOptions.AUTO_PK, YdbFieldType.Text, false));
        return YdbOptions.AUTO_PK;
    }

    private YdbStoreType getStoreType(Map<String, String> properties) {
        String value = properties.get(YdbOptions.TABLE_TYPE);
        if (value == null) {
            return YdbStoreType.UNSPECIFIED;
        }
        return YdbStoreType.valueOf(value.toUpperCase());
    }

}
