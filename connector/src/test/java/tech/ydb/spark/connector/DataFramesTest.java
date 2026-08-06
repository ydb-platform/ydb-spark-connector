package tech.ydb.spark.connector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tech.ydb.table.description.TableDescription;
import tech.ydb.test.junit4.YdbHelperRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class DataFramesTest {
    @ClassRule
    public static final YdbHelperRule YDB = new YdbHelperRule();

    private static Map<String, String> ydbCreds;
    private static SparkSession spark;

    @BeforeClass
    public static void prepare() {
        ydbCreds = new HashMap<>();
        ydbCreds.put("url", new StringBuilder()
                .append(YDB.useTls() ? "grpcs://" : "grpc://")
                .append(YDB.endpoint())
                .append(YDB.database())
                .toString());

        if (YDB.authToken() != null) {
            ydbCreds.put("auth.token", YDB.authToken());
        }

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("ydb-spark-dataframes-test")
                .set("spark.ui.enabled", "false");

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

    private static DataFrameReader readYdb() {
        return spark.read().format("ydb").options(ydbCreds);
    }

    private static void prepareTables() {
        readYdb().option("query", ""
                + "CREATE TABLE row_table ("
                + " id Int32 NOT NULL,"
                + " value Text NOT NULL,"
                + " PRIMARY KEY(id)  "
                + ")").load().count();

        readYdb().option("query", ""
                + "CREATE TABLE column_table ("
                + " id Int32 NOT NULL,"
                + " value Text,"
                + " PRIMARY KEY(id)  "
                + ") WITH (STORE=COLUMN)").load().count();

        readYdb().option("query", ""
                + "CREATE TABLE `dir/splitted` ("
                + " id Int32 NOT NULL,"
                + " value Text,"
                + " PRIMARY KEY(id)  "
                + ") WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 7, "
                + "  PARTITION_AT_KEYS = (11, 22, 33, 44, 55, 66) "
                + ")").load().count();

        StructType schema = new StructType(new StructField[]{
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("value", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> initValues = spark.createDataFrame(Arrays.asList(
                new GenericRowWithSchema(new Object[]{1, "v1"}, schema),
                new GenericRowWithSchema(new Object[]{2, "v2"}, schema),
                new GenericRowWithSchema(new Object[]{10, "v3"}, schema),
                new GenericRowWithSchema(new Object[]{11, "v4"}, schema),
                new GenericRowWithSchema(new Object[]{12, "v5"}, schema),
                new GenericRowWithSchema(new Object[]{50, "v6"}, schema),
                new GenericRowWithSchema(new Object[]{51, "v7"}, schema),
                new GenericRowWithSchema(new Object[]{65, "v8"}, schema),
                new GenericRowWithSchema(new Object[]{66, "v9"}, schema),
                new GenericRowWithSchema(new Object[]{67, "v10"}, schema)
        ), schema);

        initValues.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("row_table");
        initValues.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("column_table");
        initValues.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("dir/splitted");
    }

    private static void cleanTables() {
        readYdb().option("query", "DROP TABLE `dir/splitted`;").load().count();
        readYdb().option("query", "DROP TABLE column_table;").load().count();
        readYdb().option("query", "DROP TABLE row_table;").load().count();

        try (YdbContext ctx = new YdbContext(ydbCreds)) {
            ctx.getExecutor().removeDirectory("dir");
            ctx.getExecutor().removeDirectory("copy");
        }
    }

    private TableDescription describeTable(String path) {
        try (YdbContext ctx = new YdbContext(ydbCreds)) {
            return ctx.getExecutor().describeTable(path, false);
        }
    }

    @Test
    public void countRowTableTest() {
        long count = readYdb().option("dbtable", "row_table").load().count();
        Assert.assertEquals(10, count);

        long count2 = readYdb().load("row_table").count();
        Assert.assertEquals(count, count2);

        long count3 = readYdb().option("useReadTable", "true")
                .load("row_table").count();
        Assert.assertEquals(count2, count3);
    }

    @Test
    public void countColumnTableTest() {
        long count = readYdb().option("dbtable", "column_table").load().count();
        Assert.assertEquals(10, count);

        long count2 = readYdb().load("column_table").count();
        Assert.assertEquals(count, count2);
    }

    @Test
    public void countSplittedTableTest() {
        long count = readYdb().option("dbtable", "dir/splitted").load().count();
        Assert.assertEquals(10, count);

        long count2 = readYdb().load("dir/splitted").count();
        Assert.assertEquals(count, count2);

        long count3 = readYdb().option("useReadTable", "true")
                .load("dir/splitted").count();
        Assert.assertEquals(count2, count3);
    }

    @Test
    public void emptyWriteTest() {
        Dataset<Row> origin = readYdb().load("row_table");

        Assert.assertEquals(10, origin.count());

        spark.createDataFrame(Collections.emptyList(), origin.schema())
                .write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("row_table");

        Assert.assertEquals(10, readYdb().load("row_table").count());
    }

    @Test
    public void tableAutoCreateTest() {
        Dataset<Row> origin = readYdb().load("row_table");

        Assert.assertEquals(10, origin.count());
        Assert.assertEquals(2, origin.schema().length());
        Assert.assertArrayEquals(new String[] {"id", "value"} , origin.schema().fieldNames());

        try {
            origin.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("copy/row_table1");
            Dataset<Row> copy = readYdb().load("copy/row_table1");

            Assert.assertEquals(10, copy.count());
            Assert.assertArrayEquals(new String[] {"id", "value", "_spark_key"} , copy.schema().fieldNames());
        } finally {
            readYdb().option("query", "DROP TABLE `copy/row_table1`;").load().count();
        }
    }

    @Test
    public void tableAutoCreateWithKeysTest() {
        Dataset<Row> origin = readYdb().load("row_table");

        Assert.assertEquals(10, origin.count());
        Assert.assertEquals(2, origin.schema().length());
        Assert.assertArrayEquals(new String[] {"id", "value"} , origin.schema().fieldNames());

        try {
            origin.write().format("ydb")
                    .options(ydbCreds)
                    .option("table.primary_keys", "value, id")
                    .mode(SaveMode.Append)
                    .save("copy/row_table2");
            Dataset<Row> copy = spark.read().format("ydb")
                    .options(ydbCreds)
                    .load("copy/row_table2");

            Assert.assertEquals(10, copy.count());
            Assert.assertArrayEquals(new String[] {"id", "value"} , copy.schema().fieldNames());

            TableDescription desc = describeTable("copy/row_table2");
            Assert.assertEquals(TableDescription.StoreType.ROW, desc.getStoreType());
        } finally {
            readYdb().option("query", "DROP TABLE `copy/row_table2`;").load().count();
        }
    }

    @Test
    public void tableAutoCreateColumnTableTest() {
        Dataset<Row> origin = readYdb().load("row_table");

        Assert.assertEquals(10, origin.count());
        Assert.assertEquals(2, origin.schema().length());
        Assert.assertArrayEquals(new String[] {"id", "value"} , origin.schema().fieldNames());

        try {
            origin.write().format("ydb")
                    .options(ydbCreds)
                    .option("table.primary_keys", " , value,,,")
                    .option("table.type", "column")
                    .mode(SaveMode.Append)
                    .save("copy/column_table");
            Dataset<Row> copy = spark.read().format("ydb")
                    .options(ydbCreds)
                    .load("copy/column_table");

            Assert.assertEquals(10, copy.count());
            Assert.assertArrayEquals(new String[] {"id", "value"} , copy.schema().fieldNames());

            TableDescription desc = describeTable("copy/column_table");
            Assert.assertEquals(TableDescription.StoreType.COLUMN, desc.getStoreType());
        } finally {
            readYdb().option("query", "DROP TABLE `copy/column_table`;").load().count();
        }
    }

    @Test
    public void indexedTableAutoUpsertTest() {
        Dataset<Row> origin = readYdb().load("row_table");
        Assert.assertEquals(10, origin.count());
        Assert.assertEquals(2, origin.schema().length());
        Assert.assertArrayEquals(new String[] {"id", "value"} , origin.schema().fieldNames());

        readYdb().option("query", "CREATE TABLE `copy/indexed_table` ("
                + " id Int32 NOT NULL,"
                + " value Text NOT NULL,"
                + " PRIMARY KEY(id),  "
                + " INDEX value_idx GLOBAL SYNC ON (value) "
                + ")").load().count();

        try {
            origin.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("copy/indexed_table");
            Dataset<Row> copy = readYdb().load("copy/indexed_table");

            Assert.assertEquals(10, copy.count());
            Assert.assertArrayEquals(new String[] {"id", "value" } , copy.schema().fieldNames());
        } finally {
            readYdb().option("query", "DROP TABLE `copy/indexed_table`;").load().count();
        }
    }
}
