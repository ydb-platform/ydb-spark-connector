package tech.ydb.spark.connector.common;


import java.util.Map;

import tech.ydb.table.settings.PartitioningSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public enum PartitionOption {
    AUTO_PARTITIONING_BY_SIZE,
    AUTO_PARTITIONING_BY_LOAD,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT,
    AUTO_PARTITIONING_PARTITION_SIZE_MB;

    public void apply(PartitioningSettings settings, String value) {
        switch (this) {
            case AUTO_PARTITIONING_BY_SIZE:
                if (value == null || value.isEmpty()) {
                    settings.clearPartitioningBySize();
                } else {
                    settings.setPartitioningBySize(SparkOption.parseBoolean(name(), value.trim()));
                }
                break;
            case AUTO_PARTITIONING_BY_LOAD:
                if (value == null || value.isEmpty()) {
                    settings.clearPartitioningByLoad();
                } else {
                    settings.setPartitioningByLoad(SparkOption.parseBoolean(name(), value.trim()));
                }
                break;
            case AUTO_PARTITIONING_MIN_PARTITIONS_COUNT:
                if (value == null || value.isEmpty()) {
                    settings.clearMinPartitionsCount();
                } else {
                    settings.setMinPartitionsCount(SparkOption.parseLong(name(), value.trim()));
                }
                break;
            case AUTO_PARTITIONING_MAX_PARTITIONS_COUNT:
                if (value == null || value.isEmpty()) {
                    settings.clearMaxPartitionsCount();
                } else {
                    settings.setMaxPartitionsCount(SparkOption.parseLong(name(), value.trim()));
                }
                break;
            case AUTO_PARTITIONING_PARTITION_SIZE_MB:
                if (value == null || value.isEmpty()) {
                    settings.clearPartitionSize();
                } else {
                    settings.setPartitionSize(SparkOption.parseLong(name(), value.trim()));
                }
                break;
            default:
                throw new AssertionError("Unsupported option " + this);
        }
    }

    public static void writeAll(Map<String, String> options, PartitioningSettings ps) {
        if (ps.getPartitioningBySize() != null) {
            options.put(AUTO_PARTITIONING_BY_SIZE.name(), ps.getPartitioningBySize() ? "ENABLED" : "DISABLED");
        }
        if (ps.getPartitioningByLoad() != null) {
            options.put(AUTO_PARTITIONING_BY_LOAD.name(), ps.getPartitioningByLoad() ? "ENABLED" : "DISABLED");
        }
        if (ps.getPartitionSizeMb() != null) {
            options.put(AUTO_PARTITIONING_PARTITION_SIZE_MB.name(), String.valueOf(ps.getPartitionSizeMb()));
        }
        if (ps.getMinPartitionsCount() != null) {
            options.put(AUTO_PARTITIONING_MIN_PARTITIONS_COUNT.name(), String.valueOf(ps.getMinPartitionsCount()));
        }
        if (ps.getMaxPartitionsCount() != null) {
            options.put(AUTO_PARTITIONING_MAX_PARTITIONS_COUNT.name(), String.valueOf(ps.getMaxPartitionsCount()));
        }
    }

    private boolean read(Map<String, String> options, boolean defvalue) {
        if (!options.containsKey(name())) {
            return defvalue;
        }
        return SparkOption.parseBoolean(name(), options.get(name()));
    }

    private long read(Map<String, String> options, long defvalue) {
        if (!options.containsKey(name())) {
            return defvalue;
        }
        return SparkOption.parseLong(name(), options.get(name()));
    }

    public static PartitioningSettings readAll(Map<String, String> options) {
        PartitioningSettings ps = new PartitioningSettings();
        ps.setPartitioningBySize(AUTO_PARTITIONING_BY_SIZE.read(options, true));
        ps.setPartitioningByLoad(AUTO_PARTITIONING_BY_LOAD.read(options, true));

        long minPartitions = AUTO_PARTITIONING_MIN_PARTITIONS_COUNT.read(options, 1L);
        if (minPartitions < 1) {
            minPartitions = 1L;
        }
        long maxPartitions = AUTO_PARTITIONING_MAX_PARTITIONS_COUNT.read(options, 50L);
        if (maxPartitions < minPartitions) {
            maxPartitions = minPartitions + 49L + (minPartitions / 100L);
        }
        ps.setMinPartitionsCount(minPartitions);
        ps.setMaxPartitionsCount(maxPartitions);

        long minSizeMb = AUTO_PARTITIONING_PARTITION_SIZE_MB.read(options, 1000L);
        if (minSizeMb < 10L) {
            minSizeMb = 10L;
        }
        ps.setPartitionSize(minSizeMb);

        return ps;
    }
}
