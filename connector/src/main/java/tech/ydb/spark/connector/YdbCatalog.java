package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.scheme.description.DescribePathResult;
import tech.ydb.scheme.description.Entry;
import tech.ydb.scheme.description.EntryType;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.spark.connector.impl.AlterTableBuilder;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;

/**
 * YDB Catalog implements Spark table catalog for YDB data sources.
 *
 * @author zinal
 */
public class YdbCatalog implements CatalogPlugin, TableCatalog, SupportsNamespaces {

    private static final Logger logger = LoggerFactory.getLogger(YdbCatalog.class);

    public static final String INDEX_PREFIX = "ix/";
    public static final String ENTRY_TYPE = "ydb_entry_type";
    public static final String ENTRY_OWNER = "ydb_entry_owner";

    private String name;
    private YdbContext ctx;
    private YdbTypes types;
    private boolean listIndexes;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        logger.info("Initialize YDB catalog {}", name);
        this.name = name;
        this.ctx = new YdbContext(options);
        this.types = new YdbTypes(options);
        this.listIndexes = OperationOption.LIST_INDEXES.readBoolean(options, false);
    }

    @Override
    public String name() {
        return name;
    }

    public static <T> T checkStatus(Result<T> res, String[] namespace) throws NoSuchNamespaceException {
        if (!res.isSuccess()) {
            final Status status = res.getStatus();
            if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                for (Issue i : status.getIssues()) {
                    if (i != null && i.getMessage().endsWith("Path not found")) {
                        throw new NoSuchNamespaceException(namespace);
                    }
                }
            }
            status.expectSuccess("ydb metadata query failed on " + Arrays.toString(namespace));
        }
        return res.getValue();
    }

    public static <T> T checkStatus(Result<T> res, Identifier id) throws NoSuchTableException {
        if (!res.isSuccess()) {
            Status status = res.getStatus();
            if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                throw new NoSuchTableException(id);
            }
            status.expectSuccess("ydb metadata query failed on " + id);
        }
        return res.getValue();
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        String path = String.join("/", namespace);
        ListDirectoryResult list = ctx.getExecutor().listDirectory(path);
        if (list == null) {
            throw new NoSuchNamespaceException(namespace);
        }

        List<Identifier> retval = new ArrayList<>();
        for (Entry e : list.getEntryChildren()) {
            if (e.getType() == EntryType.TABLE) {
                retval.add(Identifier.of(namespace, e.getName()));
                if (listIndexes) {
                    String tablePath = path + "/" + e.getName();
                    TableDescription td = ctx.getExecutor().describeTable(tablePath, false);
                    if (td != null) {
                        for (TableIndex ix : td.getIndexes()) {
                            String ixname = INDEX_PREFIX + e.getName() + "/" + ix.getName();
                            retval.add(Identifier.of(namespace, ixname));
                        }
                    }
                }
            }
            if (e.getType() == EntryType.COLUMN_TABLE) {
                retval.add(Identifier.of(namespace, e.getName()));
            }
        }
        return retval.toArray(new Identifier[0]);
    }

    @Override
    public YdbTable loadTable(Identifier ident) throws NoSuchTableException {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            // Special support for index "tables".
            String pseudoName = ident.name();
            String[] tabParts = pseudoName.split("[/]");
            if (tabParts.length != 3) {
                // Illegal name format - so "no such table".
                throw new NoSuchTableException(ident);
            }
            String realName = tabParts[1] + "/" + tabParts[2] + YdbTable.INDEX_TABLE_NAME;
            String tablePath = ctx.getExecutor().extractPath(toPath(Identifier.of(ident.namespace(), realName)));
            TableDescription td = ctx.getExecutor().describeTable(tablePath, true);
            if (td == null) {
                throw new NoSuchTableException(ident);
            }
            return new YdbTable(ctx, types, toPath(ident), tablePath, td);
        }
        // Processing for regular tables.
        String tablePath = ctx.getExecutor().extractPath(toPath(ident));
        TableDescription td = ctx.getExecutor().describeTable(tablePath, true);
        if (td == null) {
            throw new NoSuchTableException(ident);
        }

        return new YdbTable(ctx, types, tablePath, tablePath, td);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
            Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Direct index table creation is not possible,"
                    + "identifier " + ident);
        }

        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
        String tablePath = ctx.getExecutor().extractPath(toPath(ident));
        TableDescription td = YdbTable.buildTableDesctiption(types.fromSparkSchema(schema), options);
        ctx.getExecutor().createTable(tablePath, td);

        // describe table to get information about shards
        TableDescription created = ctx.getExecutor().describeTable(tablePath, true);
        return new YdbTable(ctx, types, tablePath, tablePath, created);
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Index table alteration is not possible, "
                    + "identifier " + ident);
        }

        String tablePath = ctx.getExecutor().extractPath(toPath(ident));
        TableDescription td = ctx.getExecutor().describeTable(tablePath, false);

        if (td == null) {
            throw new NoSuchTableException(ident);
        }

        AlterTableBuilder builder = new AlterTableBuilder(types, td);
        for (TableChange change : changes) {
            if (change instanceof TableChange.AddColumn) {
                builder.prepare((TableChange.AddColumn) change);
            } else if (change instanceof TableChange.DeleteColumn) {
                builder.prepare((TableChange.DeleteColumn) change);
            } else if (change instanceof TableChange.SetProperty) {
                builder.prepare((TableChange.SetProperty) change);
            } else if (change instanceof TableChange.RemoveProperty) {
                builder.prepare((TableChange.RemoveProperty) change);
            } else {
                throw new UnsupportedOperationException("YDB table alter operation not supported: " + change);
            }
        }

        ctx.getExecutor().alterTable(tablePath, builder.build());

        // describe table to get information about shards
        TableDescription update = ctx.getExecutor().describeTable(tablePath, true);
        return new YdbTable(ctx, types, tablePath, tablePath, update);
    }

    @Override
    public boolean dropTable(Identifier ident) {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Cannot drop index table " + ident);
        }

        final String tablePath = toPath(ident);
        logger.debug("Dropping table {}", tablePath);
        return ctx.getExecutor().dropTable(tablePath);
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        if (oldIdent.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Cannot rename index table " + oldIdent);
        }
        if (newIdent.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Cannot rename table to index " + newIdent);
        }
        final String oldPath = toPath(oldIdent);
        final String newPath = toPath(newIdent);
        ctx.getExecutor().renameTable(oldPath, newPath);
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return listNamespaces(null);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace == null) {
            namespace = new String[0];
        }
        ListDirectoryResult res = ctx.getExecutor().listDirectory(toPath(namespace));
        if (res == null) {
            throw new NoSuchNamespaceException(namespace);
        }

        List<String[]> retval = new ArrayList<>();
        for (Entry e : res.getEntryChildren()) {
            if (e.getType() == EntryType.DIRECTORY) {
                final String[] x = new String[namespace.length + 1];
                System.arraycopy(namespace, 0, x, 0, namespace.length);
                x[namespace.length] = e.getName();
                retval.add(x);
            }
        }
        return retval.toArray(new String[0][0]);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        if (namespace == null || namespace.length == 0) {
            return Collections.emptyMap();
        }
        DescribePathResult res = ctx.getExecutor().describeDirectory(toPath(namespace));
        if (res == null) {
            throw new NoSuchNamespaceException(namespace);
        }

        final Map<String, String> m = new HashMap<>();
        m.put(ENTRY_TYPE, res.getEntry().getType().name());
        m.put(ENTRY_OWNER, res.getEntry().getOwner());
        return m;
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        if (!ctx.getExecutor().makeDirectory(toPath(namespace))) {
            throw new NamespaceAlreadyExistsException(namespace);
        }
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean recursive)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        if (!recursive) {
            return ctx.getExecutor().removeDirectory(toPath(namespace));
        } else {
            // TODO: recursive removal
            throw new UnsupportedOperationException("Recursive namespace removal is not implemented");
        }
    }

    private static String safeName(String v) {
        if (v == null) {
            return "";
        }
        if (v.contains("/")) {
            v = v.replace("/", "_");
        }
        if (v.contains("\\")) {
            v = v.replace("\\", "_");
        }
        return v;
    }

    private static String toPath(Identifier id) {
        StringBuilder sb = new StringBuilder();
        addToPath(sb, id.namespace());
        addToPath(sb, id.name());
        return sb.toString();
    }

    private static String toPath(String[] namespace) {
        StringBuilder sb = new StringBuilder();
        addToPath(sb, namespace);
        return sb.toString();
    }

    private static void addToPath(StringBuilder sb, String... items) {
        for (String item: items) {
            if (sb.length() > 0) {
                sb.append("/");
            }
            sb.append(safeName(item));
        }
    }
}
