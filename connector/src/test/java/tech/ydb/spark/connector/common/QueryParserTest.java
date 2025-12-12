package tech.ydb.spark.connector.common;

import org.junit.Assert;
import org.junit.Test;

import tech.ydb.spark.connector.impl.QueryParser;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryParserTest {

    @Test
    public void noSchemaTest() {
        // Invalid queries
        noSchema("ALTERED;");
        noSchema("SCANER SELECT 1;");
        noSchema("bulked select 1;");
        noSchema("select1;");
        noSchema("\ndrops;");
        noSchema("BuLK_INSERT;");
//        noSchema("SELECT * FROM table WHERE id = '");
//        noSchema("SELECT * FROM table WHERE id = \"");
//        noSchema("SELECT * FROM table WHERE id = `;");

        // DDL operations
        noSchema("  Alter table set;");
        noSchema("Alter--comment\ntable set;");
        noSchema("drOp table 'test'");
        noSchema("Revoke--comment\npermission;");
        noSchema("GRant table 'test'");
        noSchema("-- comment \nCreate;");

        // Multipli results
        noSchema("Select 1; Select 2");
        noSchema("Select 1;\nSelect 2;\n");

        // Mixed queries
        noSchema("Select 1; /* test */ INSERT into table2");
        noSchema("ALTER TABLE; Select 1;");
    }

    @Test
    public void schemeTest() {
        checkSchema("SELECT 1 + 2", "SELECT * FROM (SELECT 1 + 2) WHERE 1 = 0");

        checkSchema(
                "$select = SELECT 1 + 2; SELECT * FROM $select",
                "$select = SELECT 1 + 2; SELECT * FROM (SELECT * FROM $select) WHERE 1 = 0"
        );

        checkSchema(
                "$select = SELECT 1 + 2; SELECT * FROM (SELECT * FROM $select);",
                "$select = SELECT 1 + 2; SELECT * FROM (SELECT * FROM (SELECT * FROM $select)) WHERE 1 = 0;"
        );

        checkSchema(
                "sElEcT 'SELECT 1 \\\'+ 2' as Result FROM test table",
                "SELECT * FROM (sElEcT 'SELECT 1 \\\'+ 2' as Result FROM test table) WHERE 1 = 0"
        );

        checkSchema(
                "select * from test WHERE query=\"SELECT 1 + 2\"",
                "SELECT * FROM (select * from test WHERE query=\"SELECT 1 + 2\") WHERE 1 = 0"
        );

        checkSchema(
                "--select * from test WHERE query=\"SELECT 2 + 1\";\n"
                + "select * from test WHERE query=\"SELECT 1 + 2\";\n",
                "--select * from test WHERE query=\"SELECT 2 + 1\";\n"
                + "SELECT * FROM (select * from test WHERE query=\"SELECT 1 + 2\") WHERE 1 = 0;\n"
        );
    }

    private void noSchema(String query) {
        Assert.assertNull("Query [" + query + "] must be failed", QueryParser.describeYQL(query));
    }

    private void checkSchema(String query, String scheme) {
        Assert.assertEquals("Query [" + query + "] must be ok", scheme, QueryParser.describeYQL(query));
    }
}
