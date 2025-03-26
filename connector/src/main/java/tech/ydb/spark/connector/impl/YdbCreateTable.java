package tech.ydb.spark.connector.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Status;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.spark.connector.common.FieldType;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.spark.connector.common.PartitionOption;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.PartitioningSettings;

/**
 * Create table implementation.
 *
 * @author zinal
 */
public class YdbCreateTable {

    private static final Logger LOG = LoggerFactory.getLogger(YdbCreateTable.class);

    private final String tablePath;
    private final List<FieldInfo> fields;
    private final List<String> primaryKey;
    private final PartitioningSettings partitioningSettings;

    public YdbCreateTable(String tablePath, List<FieldInfo> fields, List<String> primaryKey,
            Map<String, String> options) {
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = primaryKey;
        this.partitioningSettings = PartitionOption.readAll(options);
    }

    public YdbCreateTable(String tablePath, List<FieldInfo> fields, Map<String, String> options) {
        this.tablePath = tablePath;
        this.fields = fields;
        this.primaryKey = makePrimaryKey(options, fields);
        this.partitioningSettings = PartitionOption.readAll(options);
    }

    public CompletableFuture<Status> createTable(Session session) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating table {} with fields {} and PK {}", tablePath, fields, primaryKey);
        }

        TableDescription.Builder tdb = TableDescription.newBuilder();
        for (FieldInfo yfi : fields) {
            if (yfi.isNullable()) {
                tdb.addNullableColumn(yfi.getName(), yfi.getType().toSdkType());
            } else {
                tdb.addNonnullColumn(yfi.getName(), yfi.getType().toSdkType());
            }
        }
        tdb.setPrimaryKeys(primaryKey);

        tdb.setPartitioningSettings(partitioningSettings);

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

    private static List<String> makePrimaryKey(Map<String, String> options, List<FieldInfo> fields) {
        String value = OperationOption.PRIMARY_KEY.read(options);
        if (value == null) {
            String autoPk = grabAutoPk(options, fields);
            return Arrays.asList(new String[] {autoPk});
        }
        return Arrays.asList(value.split("[,]"));
    }

    private static String grabAutoPk(Map<String, String> options, List<FieldInfo> fields) {
        String autoPkName = OperationOption.AUTO_PK.read(options, OperationOption.DEFAULT_AUTO_PK);
        for (FieldInfo yfi : fields) {
            if (autoPkName.equalsIgnoreCase(yfi.getName())) {
                return yfi.getName();
            }
        }
        fields.add(new FieldInfo(autoPkName, FieldType.Text, false));
        return autoPkName;
    }
}
