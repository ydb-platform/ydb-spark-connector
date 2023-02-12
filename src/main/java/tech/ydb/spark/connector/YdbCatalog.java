package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.scheme.SchemeOperationProtos;
import tech.ydb.table.SchemeClient;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.DescribePathResult;
import tech.ydb.table.description.ListDirectoryResult;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

/**
 *
 * @author mzinal
 */
public class YdbCatalog implements CatalogPlugin, TableCatalog, SupportsNamespaces {

    public static final String ENTRY_TYPE = "ydb_entry_type";
    public static final String ENTRY_OWNER = "ydb_entry_owner";

    private String catalogName;
    private YdbConnector connector;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.connector = new YdbConnector(options);
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

    private String getDatabase() {
        return getConnector().getDatabase();
    }

    private SchemeClient getSchemeClient() {
        return getConnector().getSchemeClient();
    }

    private SessionRetryContext getRetryCtx() {
        return getConnector().getRetryCtx();
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

    private static void mergeLocal(String[] items, StringBuilder sb) {
        if (items != null) {
            for (String i : items) {
                if (sb.length() > 0) sb.append("/");
                sb.append(safeName(i));
            }
        }
    }

    private String mergePath(String[] items) {
        if (items==null || items.length==0)
            return getDatabase();
        final StringBuilder sb = new StringBuilder();
        sb.append(getDatabase());
        mergeLocal(items, sb);
        return sb.toString();
    }

    private static void mergeLocal(Identifier id, StringBuilder sb) {
        mergeLocal(id.namespace(), sb);
        if (sb.length() > 0) sb.append("/");
        sb.append(safeName(id.name()));
    }

    private String mergePath(Identifier id) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getDatabase());
        mergeLocal(id, sb);
        return sb.toString();
    }

    private String mergeLocal(Identifier id) {
        final StringBuilder sb = new StringBuilder();
        mergeLocal(id, sb);
        return sb.toString();
    }

    private <T> T checkStatus(Result<T> res, String[] namespace)
            throws NoSuchNamespaceException {
        if (! res.isSuccess()) {
            final Status status = res.getStatus();
            if ( StatusCode.SCHEME_ERROR.equals(status.getCode()) ) {
                for (Issue i : status.getIssues()) {
                    if (i!=null && i.getMessage().endsWith("Path not found"))
                        throw new NoSuchNamespaceException(namespace);
                }
            }
            status.expectSuccess("ydb.listDirectory failed on " + Arrays.toString(namespace));
        }
        return res.getValue();
    }

    private <T> T checkStatus(Result<T> res, Identifier id)
            throws NoSuchTableException {
        if (! res.isSuccess()) {
            final Status status = res.getStatus();
            if ( StatusCode.SCHEME_ERROR.equals(status.getCode()) ) {
                for (Issue i : status.getIssues()) {
                    if (i!=null && i.getMessage().endsWith("Path not found"))
                        throw new NoSuchTableException(id);
                }
            }
            status.expectSuccess("ydb.listDirectory failed on " + id);
        }
        return res.getValue();
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
                }
            }
            return retval.toArray(new Identifier[0]);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        Result<TableDescription> res = getRetryCtx().supplyResult(session -> {
            final DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeTableStats(true);
            return session.describeTable(mergePath(ident), dts);
        }).join();
        TableDescription td = checkStatus(res, ident);
        return new YdbTable(getConnector(), mergeLocal(ident), td);
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, 
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return listNamespaces(null);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
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
        m.put(ENTRY_TYPE, dpr.getSelf().getOwner());
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
    public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
        // TODO: recursive removal
        Status status = getSchemeClient().removeDirectory(mergePath(namespace)).join();
        return status.isSuccess();
    }

}
