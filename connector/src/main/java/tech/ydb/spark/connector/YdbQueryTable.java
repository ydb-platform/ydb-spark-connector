package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.spark.connector.read.YdbQueryScan;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryTable implements Serializable, Table, SupportsRead {
    private static final long serialVersionUID = 7550239461285485720L;

    private final YdbContext ctx;
    private final String query;
    private final StructType schema;

    public YdbQueryTable(YdbContext ctx, String query, StructType schema) {
        this.ctx = ctx;
        this.query = query;
        this.schema = schema;
    }

    public YdbContext getCtx() {
        return ctx;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public String name() {
        return "YQL[" + query + "]";
    }

    @Override
    @SuppressWarnings("deprecation")
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new YdbQueryScan(this, options);
    }
}
