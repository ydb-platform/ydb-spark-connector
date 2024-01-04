package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import scala.collection.JavaConverters;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import tech.ydb.core.Status;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.PartitioningSettings;

/**
 *
 * @author zinal
 */
class YdbCreateTable implements Runnable {

    private final SessionRetryContext retryCtx;

    private final String tablePath;
    private final List<YdbFieldInfo> fields;
    private final List<String> primaryKey;
    private final Map<String,String> properties;

    public YdbCreateTable(SessionRetryContext retryCtx, String tablePath,
            List<YdbFieldInfo> fields, List<String> primaryKey, Map<String,String> properties) {
        this.retryCtx = retryCtx;
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = primaryKey;
        this.properties = properties;
    }

    public YdbCreateTable(SessionRetryContext retryCtx, String tablePath,
            List<YdbFieldInfo> fields, Map<String,String> properties) {
        this.retryCtx = retryCtx;
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = makePrimaryKey(fields, properties);
        this.properties = properties;
    }

    @Override
    public void run() {
        retryCtx.supplyStatus(session -> createTable(session)).join()
                .expectSuccess("failed to create table [" + tablePath + "]");
    }

    private CompletableFuture<Status> createTable(Session session) {
        TableDescription.Builder tdb = TableDescription.newBuilder();
        for (YdbFieldInfo yfi : fields) {
            if (yfi.isNullable()) {
                tdb.addNullableColumn(yfi.getName(), YdbFieldType.toSdkType(yfi.getType(), false));
            } else {
                tdb.addNonnullColumn(yfi.getName(), YdbFieldType.toSdkType(yfi.getType(), false));
            }
        }
        tdb.setPrimaryKeys(primaryKey);

        final PartitioningSettings ps = new PartitioningSettings();
        ps.setPartitioningBySize(getBooleanOption("AUTO_PARTITIONING_BY_SIZE", true));
        ps.setPartitioningByLoad(getBooleanOption("AUTO_PARTITIONING_BY_LOAD", true));
        long minPartitions = getLongOption("AUTO_PARTITIONING_MIN_PARTITIONS_COUNT", 1L);
        if (minPartitions < 1) {
            minPartitions = 1L;
        }
        long maxPartitions = getLongOption("AUTO_PARTITIONING_MAX_PARTITIONS_COUNT", 50L);
        if (maxPartitions < minPartitions + 50L) {
            maxPartitions = minPartitions + 50L + (minPartitions / 100L);
        }
        ps.setMinPartitionsCount(minPartitions);
        ps.setMaxPartitionsCount(maxPartitions);
        long minSizeMb = getLongOption("AUTO_PARTITIONING_PARTITION_SIZE_MB", 2000L);
        if (minSizeMb < 1L) {
            minSizeMb = 10L;
        }
        ps.setPartitionSize(minSizeMb);
        tdb.setPartitioningSettings(ps);

        return session.createTable(tablePath, tdb.build());
    }

    private boolean getBooleanOption(String name, boolean defval) {
        if (properties != null) {
            name = name.trim().toLowerCase();
            String value = properties.get(name);
            if (value!=null) {
                value = value.trim();
                if (value.length() > 0) {
                    if ("1".equalsIgnoreCase(value)
                            || "yes".equalsIgnoreCase(value)
                            || "true".equalsIgnoreCase(value)
                            || "enabled".equalsIgnoreCase(value)) {
                        return true;
                    }
                    if ("0".equalsIgnoreCase(value)
                            || "no".equalsIgnoreCase(value)
                            || "false".equalsIgnoreCase(value)
                            || "disabled".equalsIgnoreCase(value)) {
                        return false;
                    }
                    throw new IllegalArgumentException("Illegal value [" + value + "] "
                            + "for property " + name);
                }
            }
        }
        return defval;
    }

    private long getLongOption(String name, long defval) {
        if (properties != null) {
            name = name.trim().toLowerCase();
            String value = properties.get(name);
            if (value!=null) {
                value = value.trim();
                if (value.length() > 0) {
                    try {
                        return Long.parseLong(value);
                    } catch(NumberFormatException nfe) {
                        throw new IllegalArgumentException("Illegal value [" + value + "] "
                                + "for property " + name, nfe);
                    }
                }
            }
        }
        return defval;
    }

    public static List<YdbFieldInfo> convert(StructType st) {
        final List<YdbFieldInfo> fields = new ArrayList<>(st.size());
        for (StructField sf : JavaConverters.asJavaCollection(st)) {
            fields.add(new YdbFieldInfo(sf.name(), YdbFieldType.Bytes, sf.nullable()));
        }
        return fields;
    }

    public static YdbFieldType convert(YdbTypes types, org.apache.spark.sql.types.DataType type) {
        YdbFieldType yft = types.mapTypeSpark2Ydb(type);
        if (yft==null) {
            throw new IllegalArgumentException("Spark type " + type + " cannot be converted to YDB type");
        }
        return yft;
    }

    public static List<String> makePrimaryKey(List<YdbFieldInfo> fields, Map<String,String> properties) {
        String value = properties.get("primary_key");
        if (value==null) {
            value = fields.get(0).getName();
        }
        return Arrays.asList(value.split("[,]"));
    }

    public static Map<String,String> makeProperties(Map<String,String> input) {
        Map<String,String> m = new HashMap<>();
        if (input != null) {
            for (Map.Entry<String,String> me : input.entrySet()) {
                m.put(me.getKey().toLowerCase(), me.getValue());
            }
        }
        return m;
    }

}
