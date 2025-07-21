package tech.ydb.spark.connector;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tech.ydb.spark.connector.impl.YdbExecutor;
import tech.ydb.test.junit4.YdbHelperRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SparkSqlTest {
    @ClassRule
    public static final YdbHelperRule YDB = new YdbHelperRule();

    private static String ydbURL;
    private static YdbContext ctx;
    private static SparkSession spark;

    @BeforeClass
    public static void prepare() {
        StringBuilder url = new StringBuilder()
                .append(YDB.useTls() ? "grpcs://" : "grpc://")
                .append(YDB.endpoint())
                .append(YDB.database());

        if (YDB.authToken() != null) {
            url.append("?").append("token=").append(YDB.authToken());
        }

        ydbURL = url.toString();
        ctx = new YdbContext(Collections.singletonMap("url", ydbURL));

        prepareTables(ctx.getExecutor());

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("ydb-spark-sql-test")
                .set("spark.ui.enabled", "false")
                .set("spark.sql.catalog.ydb", "tech.ydb.spark.connector.YdbCatalog")
                .set("spark.sql.catalog.ydb.url", ydbURL);

        spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    @AfterClass
    public static void close() {
        if (spark != null) {
            spark.close();
        }
        if (ctx != null) {
            cleanTables(ctx.getExecutor());
            ctx.close();
        }
        ctx.close();
    }

    private static void prepareTables(YdbExecutor executor) {
        executor.makeDirectory(executor.extractPath("test_dir"));
        executor.makeDirectory(executor.extractPath("empty_dir"));

        executor.executeSchemeQuery("CREATE TABLE test1 (id Int32 NOT NULL, value Text, PRIMARY KEY(id))")
                .join().expectSuccess("cannot create test table");
        executor.executeSchemeQuery("CREATE TABLE `test_dir/test2` (id Int32 NOT NULL, value Text, PRIMARY KEY(id))")
                .join().expectSuccess("cannot create test table");
    }

    private static void cleanTables(YdbExecutor executor) {
        executor.executeSchemeQuery("DROP TABLE `test_dir/test2`;").join();
        executor.executeSchemeQuery("DROP TABLE test1;").join();

        executor.removeDirectory(executor.extractPath("empty_dir"));
        executor.removeDirectory(executor.extractPath("test_dir"));
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
        Assert.assertEquals(1, testDir.count());
        Row test2 = testDir.first();
        Assert.assertEquals("test_dir", test2.getAs("namespace"));
        Assert.assertEquals("test2", test2.getAs("tableName"));
        Assert.assertEquals(Boolean.FALSE, test2.getAs("isTemporary"));

        Dataset<Row> emptyDir = spark.sql("show tables from ydb.empty_dir");
        Assert.assertEquals(0, emptyDir.count());
    }

    @Test
    public void showNamespacesTest() {
        Dataset<Row> root = spark.sql("show namespaces from ydb");
        Assert.assertEquals(2, root.count());
    }

//    @Test
//    public void ddlTests() {
//        Dataset<Row> df = spark.sql("CREATE TABLE test_table1 (id Int32 NOT NULL, value Text, PRIMARY KEY(id)) USING YDB");
//        Assert.assertEquals(0, df.schema().size());
//        Assert.assertTrue(df.isEmpty());
//    }
}
