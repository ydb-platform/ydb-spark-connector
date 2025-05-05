package tech.ydb.spark.connector;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tech.ydb.spark.connector.impl.YdbExecutor;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.test.junit4.YdbHelperRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class DataFramesTest {
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
                .setAppName("ydb-spark-dataframes-test")
                .set("spark.ui.enabled", "false");

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
        executor.makeDirectory(executor.extractPath("df_test_dir"));
        executor.executeSchemeQuery("CREATE TABLE df_test_table ("
                + " id Int32 NOT NULL,"
                + " value Text,"
                + " PRIMARY KEY(id)  "
                + ")").join().expectSuccess("cannot create test table");
        executor.executeSchemeQuery("CREATE TABLE `df_test_dir/splitted_table` ("
                + " id Int32 NOT NULL,"
                + " value Text,"
                + " PRIMARY KEY(id)  "
                + ") WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 7, "
                + "  PARTITION_AT_KEYS = (11, 22, 33, 44, 55, 66) "
                + ")").join().expectSuccess("cannot create test table");

        StructType struct = StructType.of("id", PrimitiveType.Int32, "value", PrimitiveType.Text);
        ListValue initValues = ListType.of(struct).newValueOwn(
            struct.newValue("id", PrimitiveValue.newInt32(1), "value", PrimitiveValue.newText("v1")),
            struct.newValue("id", PrimitiveValue.newInt32(2), "value", PrimitiveValue.newText("v2")),
            struct.newValue("id", PrimitiveValue.newInt32(10), "value", PrimitiveValue.newText("v3")),
            struct.newValue("id", PrimitiveValue.newInt32(11), "value", PrimitiveValue.newText("v4")),
            struct.newValue("id", PrimitiveValue.newInt32(12), "value", PrimitiveValue.newText("v5")),
            struct.newValue("id", PrimitiveValue.newInt32(50), "value", PrimitiveValue.newText("v6")),
            struct.newValue("id", PrimitiveValue.newInt32(51), "value", PrimitiveValue.newText("v7")),
            struct.newValue("id", PrimitiveValue.newInt32(65), "value", PrimitiveValue.newText("v8")),
            struct.newValue("id", PrimitiveValue.newInt32(66), "value", PrimitiveValue.newText("v9")),
            struct.newValue("id", PrimitiveValue.newInt32(67), "value", PrimitiveValue.newText("v10"))
        );

        executor.executeBulkUpsert(executor.extractPath("df_test_table"), initValues).join()
                .expectSuccess("cannot insert data to df_test_dir");
        executor.executeBulkUpsert(executor.extractPath("df_test_dir/splitted_table"), initValues).join()
                .expectSuccess("cannot insert data to df_test_dir/splitted_table");
    }

    private static void cleanTables(YdbExecutor executor) {
        executor.executeSchemeQuery("DROP TABLE `df_test_dir/splitted_table`;").join();
        executor.executeSchemeQuery("DROP TABLE df_test_table;").join();
        executor.removeDirectory(executor.extractPath("df_test_dir"));
    }

    @Test
    public void readTableByOptionTest() {
        long count1 = spark.read().format("ydb")
                .option("url", ydbURL)
                .option("dbtable", "df_test_table")
                .load()
                .count();
        Assert.assertEquals(10, count1);

        long count2 = spark.read().format("ydb")
                .option("url", ydbURL)
                .option("dbtable", "df_test_dir/splitted_table")
                .load()
                .count();
        Assert.assertEquals(10, count2);
    }

    @Test
    public void readTableByNameTest() {
        long count1 = spark.read().format("ydb")
                .option("url", ydbURL)
                .load("df_test_table")
                .count();
        Assert.assertEquals(10, count1);

        long count2 = spark.read().format("ydb")
                .option("url", ydbURL)
                .load("df_test_dir/splitted_table")
                .count();
        Assert.assertEquals(10, count2);
    }
}
