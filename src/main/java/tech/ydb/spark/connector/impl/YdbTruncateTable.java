package tech.ydb.spark.connector.impl;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.settings.DescribeTableSettings;

/**
 * Truncate table emulation. Steps: (1) describe table; (2) create new empty table; (3) replace the
 * existing table with the empty one.
 *
 * @author zinal
 */
public class YdbTruncateTable {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbTruncateTable.class);

    private final String tablePath;

    public YdbTruncateTable(String tablePath) {
        this.tablePath = tablePath;
    }

    public CompletableFuture<Status> run(Session session) {
        LOG.debug("Truncating table {} ...", tablePath);
        DescribeTableSettings dts = new DescribeTableSettings();
        Result<TableDescription> dtr = session.describeTable(tablePath, dts).join();
        if (!dtr.isSuccess()) {
            LOG.debug("Cannot describe {} - {}", tablePath, dtr.getStatus());
            return CompletableFuture.completedFuture(dtr.getStatus());
        }
        TableDescription td = undressDescription(dtr.getValue());
        String tempPath = tablePath + "_" + randomSuffix();
        Status status = session.createTable(tempPath, td).join();
        if (!status.isSuccess()) {
            LOG.debug("Cannot create temporary table {} - {}", tempPath, status);
            return CompletableFuture.completedFuture(status);
        }
        LOG.debug("Created temporary table {} ...", tempPath);
        status = session.renameTable(tempPath, tablePath, true).join();
        if (status.isSuccess()) {
            LOG.debug("Truncation successful for {}.", tablePath);
        } else {
            LOG.debug("Failed to replace {} with {} - {}", tablePath, tempPath, status);
            Status tempStatus = session.dropTable(tempPath).join();
            if (!tempStatus.isSuccess()) {
                LOG.warn("Failed to drop temporary table {} - {}", tempPath, tempStatus);
            }
        }
        return CompletableFuture.completedFuture(status);
    }

    private String randomSuffix() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());
    }

    /**
     * Undress table description, omitting options that prevent table creation.
     * @param src Source table description.
     * @return Undressed table description.
     */
    private TableDescription undressDescription(TableDescription src) {
        TableDescription.Builder b = TableDescription.newBuilder();
        src.getColumns().forEach(tc -> undressColumn(b, tc));
        b.setPrimaryKeys(src.getPrimaryKeys());
        src.getIndexes().forEach(ti -> undressIndex(b, ti));
        return b.build();
    }

    private void undressColumn(TableDescription.Builder b, TableColumn tc) {
        switch (tc.getType().getKind()) {
            case OPTIONAL:
                b.addNullableColumn(tc.getName(), tc.getType().unwrapOptional());
                break;
            default:
                b.addNonnullColumn(tc.getName(), tc.getType());
        }
    }

    private void undressIndex(TableDescription.Builder b, TableIndex ti) {
        switch (ti.getType()) {
            case GLOBAL:
                if (ti.getDataColumns() != null) {
                    b.addGlobalIndex(ti.getName(), ti.getColumns(), ti.getDataColumns());
                } else {
                    b.addGlobalIndex(ti.getName(), ti.getColumns());
                }
                break;
            case GLOBAL_ASYNC:
                if (ti.getDataColumns() != null) {
                    b.addGlobalAsyncIndex(ti.getName(), ti.getColumns(), ti.getDataColumns());
                } else {
                    b.addGlobalAsyncIndex(ti.getName(), ti.getColumns());
                }
                break;
            default:
                LOG.warn("Unknown index type: {}, index {}, table {}",
                        ti.getType(), ti.getName(), tablePath);
        }
    }

}
