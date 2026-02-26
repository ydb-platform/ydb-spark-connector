package tech.ydb.spark.connector;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.result.ResultSetReader;

/**
 * YDB table provider. Registered under the "ydb" name via the following file:
 * META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
 *
 * @author zinal
 */
public class YdbTableProvider implements TableProvider, DataSourceRegister {

    private static final String SPARK_PATH_OPTION = "path";

    @Override
    public String shortName() {
        return "ydb";
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    private String extractTableName(CaseInsensitiveStringMap options) {
        // Check that table path is provided
        String table = OperationOption.DBTABLE.read(options);
        if (table != null && !table.trim().isEmpty()) {
            return table.trim();
        }

        String path = options.get(SPARK_PATH_OPTION);
        if (path != null && !path.trim().isEmpty()) {
            return path.trim();
        }

        throw new IllegalArgumentException("Missing property: " + OperationOption.DBTABLE);
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        YdbContext ctx = new YdbContext(options);
        YdbTypes types = new YdbTypes(options);

        String query = OperationOption.DBQUERY.read(options);
        if (query != null && !query.trim().isEmpty()) {
            ResultSetReader rs = ctx.getExecutor().describeQuery(query.trim());
            if (rs == null) {
                return new StructType(new StructField[0]);
            }
            return types.toSparkSchema(rs);
        }

        String tableName = extractTableName(options);
        String tablePath = ctx.getExecutor().extractPath(tableName);
        TableDescription td = ctx.getExecutor().describeTable(tablePath, false);
        if (td == null) {
            throw new RuntimeException("Table " + tablePath + " not found");
        }
        return types.toSparkSchema(td.getColumns());
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        YdbContext ctx = new YdbContext(options);

        String query = OperationOption.DBQUERY.read(options);
        if (query != null && !query.trim().isEmpty()) {
            return new Transform[0];
        }

        String tableName = extractTableName(options);
        String tablePath = ctx.getExecutor().extractPath(tableName);
        TableDescription td = ctx.getExecutor().describeTable(tablePath, true);
        if (td == null) {
            throw new RuntimeException("Table " + tablePath + " not found");
        }

        List<String> keyColumns = td.getPrimaryKeys();

        String[] keys = new String[keyColumns.size()];
        int idx = 0;
        for (String key : keyColumns) {
            keys[idx++] = key;
        }

        return new Transform[]{
            Expressions.bucket(td.getKeyRanges().size(), keys)
        };
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);

        YdbContext ctx = new YdbContext(options);

        String query = OperationOption.DBQUERY.read(options);
        if (query != null && !query.trim().isEmpty()) {
            return getQueryTable(ctx, schema, query.trim());
        }

        YdbTypes types = new YdbTypes(options);
        String tableName = extractTableName(options);
        String tablePath = ctx.getExecutor().extractPath(tableName);
        TableDescription td = ctx.getExecutor().describeTable(tablePath, true);
        if (td == null) {
            boolean useAutoCreate = OperationOption.TABLE_AUTOCREATE.readBoolean(options, true);
            if (!useAutoCreate) {
                throw new RuntimeException("Table " + tablePath + " not found");
            }

            // No such table - creating it.
            td = YdbTable.buildTableDesctiption(types.fromSparkSchema(schema), options);
            ctx.getExecutor().createTable(tablePath, td);
        }

        return new YdbTable(ctx, types, tableName, tablePath, td);
    }

    private Table getQueryTable(YdbContext ctx, StructType schema, String query) {
        return new YdbQueryTable(ctx, query, schema);
    }
}
