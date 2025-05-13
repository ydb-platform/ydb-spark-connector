package tech.ydb.spark.connector;

import java.util.Collections;

import javax.ws.rs.NotSupportedException;

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
        executor.makeDirectory(executor.extractPath("dir"));
        executor.executeSchemeQuery("CREATE TABLE row_table ("
                + " id Int32 NOT NULL,"
                + " value Text,"
                + " PRIMARY KEY(id)  "
                + ")").join().expectSuccess("cannot create test row table");
        executor.executeSchemeQuery("CREATE TABLE column_table ("
                + " id Int32 NOT NULL,"
                + " value Text,"
                + " PRIMARY KEY(id)  "
                + ") WITH (STORE=COLUMN)").join().expectSuccess("cannot create test column table");
        executor.executeSchemeQuery("CREATE TABLE `dir/splitted` ("
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

        executor.executeBulkUpsert(executor.extractPath("row_table"), initValues).join()
                .expectSuccess("cannot insert data to row_table");
        executor.executeBulkUpsert(executor.extractPath("column_table"), initValues).join()
                .expectSuccess("cannot insert data to column_table");
        executor.executeBulkUpsert(executor.extractPath("dir/splitted"), initValues).join()
                .expectSuccess("cannot insert data to dir/splitted");
    }

    private static void cleanTables(YdbExecutor executor) {
        executor.executeSchemeQuery("DROP TABLE `dir/splitted`;").join();
        executor.executeSchemeQuery("DROP TABLE column_table;").join();
        executor.executeSchemeQuery("DROP TABLE row_table;").join();
        executor.removeDirectory(executor.extractPath("dir"));
    }


    @Test
    public void countRowTableTest() {
        long count = spark.read().format("ydb").option("url", ydbURL).option("dbtable", "row_table").load().count();
        Assert.assertEquals(10, count);

        long count2 = spark.read().format("ydb").option("url", ydbURL).load("row_table").count();
        Assert.assertEquals(count, count2);
    }

    @Test(expected = NotSupportedException.class)
    public void countColumnTableTest() {
        long count = spark.read().format("ydb").option("url", ydbURL).option("dbtable", "column_table").load().count();
        Assert.assertEquals(10, count);

        long count2 = spark.read().format("ydb").option("url", ydbURL).load("column_table").count();
        Assert.assertEquals(count, count2);
    }

    @Test
    public void countSplittedTableTest() {
        long count = spark.read().format("ydb").option("url", ydbURL).option("dbtable", "dir/splitted").load().count();
        Assert.assertEquals(10, count);

        long count2 = spark.read().format("ydb").option("url", ydbURL).load("dir/splitted").count();
        Assert.assertEquals(count, count2);
    }
}
