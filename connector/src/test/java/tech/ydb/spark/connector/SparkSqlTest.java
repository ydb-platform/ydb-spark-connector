package tech.ydb.spark.connector;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tech.ydb.test.junit4.YdbHelperRule;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class SparkSqlTest {
    @ClassRule
    public static final YdbHelperRule YDB = new YdbHelperRule();

    private static final Map<String, String> ydbCreds = new HashMap<>();
    private static SparkSession spark;

    @BeforeClass
    public static void prepare() {
        ydbCreds.put("url", new StringBuilder()
                .append(YDB.useTls() ? "grpcs://" : "grpc://")
                .append(YDB.endpoint())
                .append(YDB.database())
                .append("?usePrefixPath=sql_test")
                .toString());

        if (YDB.authToken() != null) {
            ydbCreds.put("auth.token", YDB.authToken());
        }

        ydbCreds.put("method", "UPSERT");

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("ydb-spark-sql-test")
                .set("spark.ui.enabled", "false")
                .set("spark.sql.catalog.ydb", "tech.ydb.spark.connector.YdbCatalog");

        ydbCreds.forEach((key, value) -> conf.set("spark.sql.catalog.ydb." + key, value));

        spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        prepareTables();
    }

    @AfterClass
    public static void close() throws IOException {
        if (spark != null) {
            cleanTables();
            spark.close();
        }
    }

    private static void executeSchemeQuery(String query) {
        spark.read().format("ydb").options(ydbCreds).option("query", query).load().count();
    }

    private static void prepareTables() {
        try (YdbContext ctx = new YdbContext(ydbCreds)) {
            ctx.getExecutor().makeDirectory("test_dir");
            ctx.getExecutor().makeDirectory("empty_dir");
        }

        executeSchemeQuery("CREATE TABLE test1 (id Int32 NOT NULL, value Text, PRIMARY KEY(id))");
        executeSchemeQuery("CREATE TABLE `test_dir/test2` (id Int32 NOT NULL, value Text, PRIMARY KEY(id))");
        executeSchemeQuery("CREATE TABLE `test_dir/test3` (id Int32 NOT NULL, value Text, PRIMARY KEY(id), "
                + "INDEX value_idx GLOBAL SYNC ON (value))");
    }

    private static void cleanTables() {
        executeSchemeQuery("DROP TABLE IF EXISTS `test_dir/test3`");
        executeSchemeQuery("DROP TABLE IF EXISTS `test_dir/test2`");
        executeSchemeQuery("DROP TABLE IF EXISTS `test1`");

        try (YdbContext ctx = new YdbContext(ydbCreds)) {
            ctx.getExecutor().removeDirectory("empty_dir");
            ctx.getExecutor().removeDirectory("test_dir");
        }
    }

    @Test
    public void showTablesTest() {
        Dataset<Row> root = spark.sql("show tables from ydb");
        Assert.assertEquals(1, root.count());
        Row test1 = root.first();
        Assert.assertEquals("", test1.getAs("namespace"));
        Assert.assertEquals("test1", test1.getAs("tableName"));
        Assert.assertEquals(Boolean.FALSE, test1.getAs("isTemporary"));

        Dataset<Row> testDir = spark.sql("show tables from ydb.test_dir");
        Assert.assertEquals(2, testDir.count());
        Iterator<Row> it = testDir.toLocalIterator();

        Collection<String> tables = new TreeSet<>();
        Assert.assertTrue(it.hasNext());
        Row next = it.next();
        Assert.assertEquals("test_dir", next.getAs("namespace"));
        tables.add(next.getAs("tableName"));
        Assert.assertEquals(Boolean.FALSE, next.getAs("isTemporary"));

        Assert.assertTrue(it.hasNext());
        next = it.next();
        Assert.assertEquals("test_dir", next.getAs("namespace"));
        tables.add(next.getAs("tableName"));
        Assert.assertEquals(Boolean.FALSE, next.getAs("isTemporary"));

        Assert.assertFalse(it.hasNext());
        Assert.assertArrayEquals(new String[] { "test2", "test3" }, tables.toArray(new String[0]));

        Dataset<Row> emptyDir = spark.sql("show tables from ydb.empty_dir");
        Assert.assertEquals(0, emptyDir.count());
    }

    @Test
    public void showNamespacesTest() {
        Dataset<Row> root = spark.sql("show namespaces from ydb");
        Assert.assertEquals(2, root.count());
    }

    @Test
    public void insertTest() {
        Dataset<Row> insert = spark.sql("INSERT INTO ydb.test_dir.test3 (id, value) VALUES (1, 'v1'), (2, 'v2')");
        Assert.assertEquals(0, insert.count());

        Dataset<Row> select = spark.sql("SELECT * FROM ydb.test_dir.test3");
        Assert.assertEquals(2, select.count());
    }

//    @Test
//    public void ddlTests() {
//        Dataset<Row> df = spark.sql("CREATE TABLE test_table1 (id Int32 NOT NULL, value Text, PRIMARY KEY(id)) USING YDB");
//        Assert.assertEquals(0, df.schema().size());
//        Assert.assertTrue(df.isEmpty());
//    }
}
