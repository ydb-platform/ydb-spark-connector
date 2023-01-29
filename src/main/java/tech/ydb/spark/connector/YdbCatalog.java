package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import tech.ydb.core.Result;
import tech.ydb.scheme.SchemeOperationProtos;
import tech.ydb.table.description.ListDirectoryResult;

/**
 *
 * @author mzinal
 */
public class YdbCatalog implements CatalogPlugin, TableCatalog, SupportsNamespaces {

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

    private String merge(String[] items) {
        if (items==null || items.length==0)
            return getConnector().getDatabase();
        StringBuilder sb = new StringBuilder();
        sb.append(getConnector().getDatabase());
        for (String i : items) {
            if (sb.length() > 0) sb.append("/");
            sb.append(i);
        }
        return sb.toString();
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        try {
            Result<ListDirectoryResult> res = getConnector().getSchemeClient()
                    .listDirectory(merge(namespace)).get();
            if (! res.isSuccess()) {
                // TODO: handle NoSuchNamespaceException
            }
            ListDirectoryResult ldr = res.getValue();
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
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
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
    public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return listNamespaces(null);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        try {
            Result<ListDirectoryResult> res = getConnector().getSchemeClient()
                    .listDirectory(merge(namespace)).get();
            if (! res.isSuccess()) {
                // TODO: handle NoSuchNamespaceException
            }
            ListDirectoryResult ldr = res.getValue();
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
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
