package tech.ydb.spark.connector.read;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import tech.ydb.spark.connector.YdbTable;
import tech.ydb.spark.connector.common.FieldInfo;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SelectQuery implements Serializable {
    private static final long serialVersionUID = -7754646422175214023L;

    private final String tableName;
    private final ArrayList<String> predicates;
    private final ArrayList<String> expressions;
    private final ArrayList<String> groupBy;
    private final HashMap<String, Value<?>> params;

    private String withExpression;
    private long rowLimit;

    public SelectQuery(YdbTable table) {
        this.tableName = table.getTablePath();

        FieldInfo[] keys = table.getKeyColumns();
        this.predicates = new ArrayList<>(keys.length);
        for (int idx = 0; idx < table.getKeyColumns().length; idx += 1) {
            this.predicates.add("`" + keys[idx].getName() + "`");
        }

        this.expressions = new ArrayList<>();
        this.groupBy = new ArrayList<>();
        this.params = new HashMap<>();
        this.withExpression = null;
        this.rowLimit = -1;
    }

    private SelectQuery(String tableName, ArrayList<String> predicates, ArrayList<String> expressions,
            ArrayList<String> groupBy, HashMap<String, Value<?>> params, String withExpression, long rowLimit) {
        this.tableName = tableName;
        this.expressions = expressions;
        this.predicates = predicates;
        this.groupBy = groupBy;
        this.params = params;
        this.withExpression = withExpression;
        this.rowLimit = rowLimit;
    }

    public SelectQuery copy() {
        return new SelectQuery(tableName, new ArrayList<>(predicates), new ArrayList<>(expressions),
                new ArrayList<>(groupBy), new HashMap<>(params), withExpression, rowLimit);
    }

    public SelectQuery setWithExpression(String expression) {
        if (withExpression != null) {
            throw new IllegalArgumentException("Cannot rewrite WITH expression");
        }
        withExpression = expression;
        return this;
    }

    public SelectQuery addExpression(String exp) {
        if (exp != null) {
            expressions.add(exp);
        }
        return this;
    }

    public SelectQuery addExpressionWithParam(String exp, String prmName, Value<?> prmValue) {
        if (exp != null && prmValue != null) {
            expressions.add(exp);
            params.put(prmName, prmValue);
        }
        return this;
    }

    public SelectQuery withRowLimit(int limit) {
        this.rowLimit = limit;
        return this;
    }

    public SelectQuery replacePredicates(String[] predicates) {
        this.predicates.clear();
        for (String predicate: predicates) {
            this.predicates.add("`" + predicate + "`");
        }
        return this;
    }

    public String toQuery() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Value<?>> entry: params.entrySet())  {
            sb.append("DECLARE ")
                    .append(entry.getKey())
                    .append(" AS ")
                    .append(entry.getValue().getType().toString())
                    .append("; ");
        }

        sb.append("SELECT");
        char pDep = ' ';
        for (String col: predicates) {
            sb.append(pDep);
            sb.append(col);
            pDep = ',';
        }
        sb.append(" FROM `").append(tableName).append("`");

        String eDep = " WHERE ";
        for (String exp: expressions) {
            sb.append(eDep);
            sb.append(exp);
            eDep = " AND ";
        }

        if (withExpression != null) {
            sb.append(" WITH ").append(withExpression);
        }

        if (rowLimit > 0) {
            sb.append(" LIMIT ").append(rowLimit);
        }

        return sb.toString();
    }

    public Params toQueryParams() {
        return Params.copyOf(params);
    }
}
