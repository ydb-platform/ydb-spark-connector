package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.DescribePathResult;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.settings.DescribeTableSettings;

/**
 * YDB Catalog implements Spark table catalog for YDB data sources.
 *
 * @author zinal
 */
public class YdbCatalog extends YdbOptions
        implements CatalogPlugin, TableCatalog, SupportsNamespaces {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbCatalog.class);

    public static final String ENTRY_TYPE = "ydb_entry_type";
    public static final String ENTRY_OWNER = "ydb_entry_owner";

    private String catalogName;
    private YdbConnector connector;
    private boolean listIndexes;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.connector = YdbRegistry.getOrCreate(name, options);
        this.listIndexes = options.getBoolean(YDB_LIST_INDEXES, false);
    }

    @Override
    public String name() {
        return catalogName;
    }

    private YdbConnector getConnector() {
        if (connector==null)
            throw new IllegalStateException("Catalog " + catalogName + " not initialized");
        return connector;
    }

    private SchemeClient getSchemeClient() {
        return getConnector().getSchemeClient();
    }

    private SessionRetryContext getRetryCtx() {
        return getConnector().getRetryCtx();
    }

    public static <T> T checkStatus(Result<T> res, String[] namespace)
            throws NoSuchNamespaceException {
        if (! res.isSuccess()) {
            final Status status = res.getStatus();
            if ( StatusCode.SCHEME_ERROR.equals(status.getCode()) ) {
                for (Issue i : status.getIssues()) {
                    if (i!=null && i.getMessage().endsWith("Path not found"))
                        throw new NoSuchNamespaceException(namespace);
                }
            }
            status.expectSuccess("ydb metadata query failed on " + Arrays.toString(namespace));
        }
        return res.getValue();
    }

    public static <T> T checkStatus(Result<T> res, Identifier id)
            throws NoSuchTableException {
        if (!res.isSuccess()) {
            checkStatus(res.getStatus(), id);
        }
        return res.getValue();
    }

    public static void checkStatus(Status status, Identifier id)
            throws NoSuchTableException {
        if (status.isSuccess())
            return;
        if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
            for (Issue i : status.getIssues()) {
                if (i != null && i.getMessage().endsWith("Path not found")) {
                    throw new NoSuchTableException(id);
                }
            }
        }
        status.expectSuccess("ydb metadata query failed on " + id);
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        try {
            Result<ListDirectoryResult> res = getSchemeClient()
                    .listDirectory(mergePath(namespace)).join();
            ListDirectoryResult ldr = checkStatus(res, namespace);
            List<Identifier> retval = new ArrayList<>();
            for (SchemeOperationProtos.Entry e : ldr.getChildren()) {
                if (SchemeOperationProtos.Entry.Type.TABLE.equals(e.getType())) {
                    retval.add(Identifier.of(namespace, e.getName()));
                    if (listIndexes) {
                        listIndexes(namespace, retval, e);
                    }
                }
            }
            return retval.toArray(new Identifier[0]);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void listIndexes(String[] namespace, List<Identifier> retval,
            SchemeOperationProtos.Entry tableEntry) {
        String tablePath = mergePath(namespace, tableEntry.getName());
        Result<TableDescription> res = getRetryCtx().supplyResult(session -> {
            return session.describeTable(tablePath, new DescribeTableSettings());
        }).join();
        if (! res.isSuccess()) {
            // Skipping problematic entries.
            LOG.warn("Skipping index listing for table {} due to failed describe, status {}",
                    tablePath, res.getStatus());
            return;
        }
        TableDescription td = res.getValue();
        for (TableIndex ix : td.getIndexes()) {
            String ixname = "ix/" + tableEntry.getName() + "/" + ix.getName();
            retval.add(Identifier.of(namespace, ixname));
        }
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        if (ident.name().startsWith("ix/")) {
            // Special support for index "tables".
            return loadIndexTable(ident);
        }
        // Processing for regular tables.
        String tablePath = mergePath(ident);
        Result<TableDescription> res = getRetryCtx().supplyResult(session -> {
            final DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(true);
            return session.describeTable(tablePath, dts);
        }).join();
        TableDescription td = checkStatus(res, ident);
        return new YdbTable(getConnector(), mergeLocal(ident), tablePath, td);
    }

    private Table loadIndexTable(Identifier ident) throws NoSuchTableException {
        String pseudoName = ident.name();
        String[] tabParts = pseudoName.split("[/]");
        if (tabParts.length != 3) {
            // Illegal name format - so "no such table".
            throw new NoSuchTableException(ident);
        }
        String tabName = tabParts[1];
        String ixName = tabParts[2];
        String tablePath = mergePath(ident.namespace(), tabName);
        Result<YdbTable> res =  getRetryCtx().supplyResult(session -> {
            DescribeTableSettings dts = new DescribeTableSettings();
            Result<TableDescription> td_res = session.describeTable(tablePath, dts).join();
            if (! td_res.isSuccess())
                return CompletableFuture.completedFuture(Result.fail(td_res.getStatus()));
            TableDescription td = td_res.getValue();
            for (TableIndex ix : td.getIndexes()) {
                if (ixName.equals(ix.getName())) {
                    // Grab the description for secondary index table.
                    String indexPath = tablePath + "/" + ix.getName() + "/indexImplTable";
                    dts.setIncludeShardKeyBounds(true);
                    td_res = session.describeTable(indexPath, dts).join();
                    if (! td_res.isSuccess())
                        return CompletableFuture.completedFuture(Result.fail(td_res.getStatus()));
                    TableDescription td_ix = td_res.getValue();
                    // Construct the YdbTable object
                    return CompletableFuture.completedFuture( Result.success(
                            new YdbTable(getConnector(), mergeLocal(ident), tablePath, td, ix, td_ix)) );
                }
            }
            return CompletableFuture.completedFuture(
                    Result.fail(Status.of(StatusCode.SCHEME_ERROR)
                            .withIssues(Issue.of("Path not found", Issue.Severity.ERROR))));
        }).join();

        return checkStatus(res, ident);
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, 
            Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
        if (ident.name().startsWith("ix/")) {
            throw new UnsupportedOperationException("Direct index table creation is not possible,"
                    + "identifier " + ident);
        }
        String tablePath = mergePath(ident);
        // Actual table creation logic is moved to a separate class.
        new YdbCreateTable(getRetryCtx(), tablePath,
                YdbCreateTable.convert(schema), YdbCreateTable.makeProperties(properties)).run();
        // Load the description for the table created.
        Result<TableDescription> res = getRetryCtx().supplyResult(session -> {
            final DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(true);
            return session.describeTable(tablePath, dts);
        }).join();
        res.getStatus().expectSuccess();
        return new YdbTable(getConnector(), mergeLocal(ident), tablePath, res.getValue());
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        if (ident.name().startsWith("ix/")) {
            throw new UnsupportedOperationException("Index table alteration is not possible,"
                    + "identifier " + ident);
        }
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        if (ident.name().startsWith("ix/")) {
            throw new UnsupportedOperationException("Cannot drop index table " + ident);
        }
        final String tablePath = mergePath(ident);
        Result<TableDescription> res = getRetryCtx().supplyResult(session -> {
            final DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(false);
            return session.describeTable(tablePath, dts);
        }).join();
        try {
            checkStatus(res, ident);
        } catch(NoSuchTableException nste) {
            return false;
        }
        Status status = connector.getRetryCtx().supplyStatus(
                session -> session.dropTable(tablePath)).join();
        if (! status.isSuccess()) {
            status.expectSuccess("Failed to drop table " + ident);
        }
        return true;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        if (oldIdent.name().startsWith("ix/")) {
            throw new UnsupportedOperationException("Cannot rename index table " + oldIdent);
        }
        if (newIdent.name().startsWith("ix/")) {
            throw new UnsupportedOperationException("Cannot rename table to index " + newIdent);
        }
        final String oldPath = mergePath(oldIdent);
        final String newPath = mergePath(newIdent);
        Status status = getRetryCtx().supplyStatus(
                session -> session.renameTable(oldPath, newPath, false)).join();
        if ( ! status.isSuccess()) {
            status.expectSuccess("Failed to rename table [" + oldIdent + "] to [" + newIdent + "]");
        }
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return listNamespaces(null);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace==null)
            namespace = new String[0];
        try {
            Result<ListDirectoryResult> res = getSchemeClient()
                    .listDirectory(mergePath(namespace)).get();
            ListDirectoryResult ldr = checkStatus(res, namespace);
            List<String[]> retval = new ArrayList<>();
            for (SchemeOperationProtos.Entry e : ldr.getChildren()) {
                if (SchemeOperationProtos.Entry.Type.DIRECTORY.equals(e.getType())) {
                    final String[] x = new String[namespace.length + 1];
                    System.arraycopy(namespace, 0, x, 0, namespace.length);
                    x[namespace.length] = e.getName();
                    retval.add(x);
                }
            }
            return retval.toArray(new String[0][0]);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        if (namespace==null || namespace.length==0)
            return Collections.emptyMap();
        final Map<String,String> m = new HashMap<>();
        Result<DescribePathResult> res = getSchemeClient()
                .describePath(mergePath(namespace)).join();
        DescribePathResult dpr = checkStatus(res, namespace);
        m.put(ENTRY_TYPE, dpr.getSelf().getType().name());
        m.put(ENTRY_OWNER, dpr.getSelf().getOwner());
        return m;
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        Status status = getSchemeClient().makeDirectory(mergePath(namespace)).join();
        if (status.isSuccess()
                && status.getIssues() != null
                && status.getIssues().length > 0) {
            for (Issue i : status.getIssues()) {
                String msg = i.getMessage();
                if (msg!=null && msg.contains(" path exist, request accepts it"))
                    throw new NamespaceAlreadyExistsException(namespace);
            }
        }
        status.expectSuccess();
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean recursive)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        if (! recursive) {
            Status status = getSchemeClient().removeDirectory(mergePath(namespace)).join();
            return status.isSuccess();
        } else {
            // TODO: recursive removal
            throw new UnsupportedOperationException("Recursive namespace removal is not implemented");
        }
    }

    private String getDatabase() {
        return connector.getDatabase();
    }

    private static String safeName(String v) {
        if (v==null)
            return "";
        if (v.contains("/"))
            v = v.replace("/", "_");
        if (v.contains("\\"))
            v = v.replace("\\", "_");
        return v;
    }

    private void mergeLocal(String[] items, StringBuilder sb) {
        if (items != null) {
            for (String i : items) {
                if (sb.length() > 0) sb.append("/");
                sb.append(safeName(i));
            }
        }
    }

    private void mergeLocal(Identifier id, StringBuilder sb) {
        mergeLocal(id.namespace(), sb);
        if (sb.length() > 0) sb.append("/");
        sb.append(safeName(id.name()));
    }

    private String mergeLocal(Identifier id) {
        final StringBuilder sb = new StringBuilder();
        mergeLocal(id, sb);
        return sb.toString();
    }

    private String mergePath(String[] items) {
        if (items==null || items.length==0)
            return getDatabase();
        final StringBuilder sb = new StringBuilder();
        sb.append(getDatabase());
        mergeLocal(items, sb);
        return sb.toString();
    }

    private String mergePath(String[] items, String extra) {
        if (extra==null) {
            return mergePath(items);
        }
        if (items==null) {
            return mergePath(new String[] {extra});
        }
        String[] work = new String[1 + items.length];
        System.arraycopy(items, 0, work, 0, items.length);
        work[items.length] = extra;
        return mergePath(work);
    }

    private String mergePath(Identifier id) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getDatabase());
        mergeLocal(id, sb);
        return sb.toString();
    }

}
