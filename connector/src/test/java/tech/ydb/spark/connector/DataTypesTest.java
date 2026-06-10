package tech.ydb.spark.connector;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
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

import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.spark.connector.impl.YdbExecutor;
import tech.ydb.test.junit4.YdbHelperRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class DataTypesTest {
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

        ydbCreds.put("table.autocreate", "false");

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("ydb-spark-predicates-test")
                .set("spark.ui.enabled", "false");

        spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    @AfterClass
    public static void close() throws IOException {
        if (spark != null) {
            spark.close();
        }

        YdbExecutor executor = new YdbContext(ydbCreds).getExecutor();
        executor.removeDirectory(executor.extractPath("datetypes"));

        YdbRegistry.closeAll();
    }

    private static DataFrameReader readYdb() {
        return spark.read().format("ydb").options(ydbCreds);
    }

    @Test
    public void incorrectDateTest() {
        StructType schema = new StructType(new StructField[]{
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("date", DataTypes.DateType, false, Metadata.empty()),
//            new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty()),
        });

        LocalDate d1 = LocalDate.of(2025, Month.MARCH, 5);
        LocalDate d2 = LocalDate.of(1960, Month.APRIL, 4);
        LocalDate d3 = LocalDate.of(2050, Month.MAY, 25);

        ArrayList<Row> test1 = new ArrayList<>();
        test1.add(new GenericRowWithSchema(new Object[] { 1, d1 }, schema));
        test1.add(new GenericRowWithSchema(new Object[] { 2, d2 }, schema));
        test1.add(new GenericRowWithSchema(new Object[] { 3, d3 }, schema));

        Dataset<Row> df1 = spark.createDataFrame(test1, schema);

        Map<ExtendedLogger, Level> before = new HashMap<>();
        try {
            for (Class<?> clazz: new Class<?>[] {
                org.apache.spark.util.Utils.class,
                org.apache.spark.executor.Executor.class,
                org.apache.spark.sql.execution.datasources.v2.AppendDataExec.class,
                org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTask.class,
            }) {
                ExtendedLogger logger = LogManager.getContext(true).getLogger(clazz);
                before.put(logger, logger.getLevel());
                // hide logger
                Configurator.setLevel(logger, Level.OFF);
            }

            SparkException ex = Assert.assertThrows(SparkException.class,
                    () -> df1.write().format("ydb")
                            .options(ydbCreds)
                            .option("table.autocreate", true)
                            .option("table.useSignedDatetypes", false)
                            .mode(SaveMode.Append).save("datetypes/dates1")
            );
            Assert.assertTrue(ex.getCause() instanceof IllegalArgumentException);
            Assert.assertEquals("negative daysSinceEpoch: -3559", ex.getCause().getMessage());

            df1.write().format("ydb")
                    .options(ydbCreds)
                    .option("table.autocreate", true)
                    .option("table.useSignedDatetypes", true)
                    .mode(SaveMode.Append)
                    .save("datetypes/dates2");
            Assert.assertEquals(3l, readYdb().load("datetypes/dates2").count());
        } finally {
            // recover all loggers
            before.forEach((logger, level) -> Configurator.setLevel(logger, level));
            readYdb().option("query", "DROP TABLE IF EXISTS `datetypes/dates1`").load().count();
            readYdb().option("query", "DROP TABLE IF EXISTS `datetypes/dates2`").load().count();
        }
    }

    @Test
    public void writeProtobufTest() {
        try {
            TestData data = new TestData(true);

            String createTable = "CREATE TABLE `datetypes/protobuf`(" + data.toYqlColumns() + "PRIMARY KEY(id));";
            readYdb().option("query", createTable).load().count();

            Dataset<Row> origin = spark.createDataFrame(data.generateSet(3000, 13000), data.getSchema());
            origin.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("datetypes/protobuf");

            TestData.assertEquals("protobuf", 10000, origin, readYdb().load("datetypes/protobuf").orderBy("id"));
        } finally {
            spark.read().format("ydb").options(ydbCreds)
                    .option("query", "DROP TABLE IF EXISTS `datetypes/protobuf`")
                    .load().count();
        }
    }

    @Test
    public void writeInsertTest() {
        Map<ExtendedLogger, Level> before = new HashMap<>();
        try {
            TestData data = new TestData(true);

            String createTable = "CREATE TABLE `datetypes/insert_test`(" + data.toYqlColumns() + "PRIMARY KEY(id));";
            readYdb().option("query", createTable).load().count();

            Dataset<Row> origin = spark.createDataFrame(data.generateSet(100, 600), data.getSchema());
            origin.write().format("ydb").options(ydbCreds)
                    .option("method", "INSERT")
                    .mode(SaveMode.Append).save("datetypes/insert_test");

            TestData.assertEquals("insert_test", 500, origin, readYdb().load("datetypes/insert_test").orderBy("id"));

            for (Class<?> clazz: new Class<?>[] {
                tech.ydb.spark.connector.write.YdbDataWriter.class,
                org.apache.spark.util.Utils.class,
                org.apache.spark.executor.Executor.class,
                org.apache.spark.sql.execution.datasources.v2.AppendDataExec.class,
                org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTask.class,
            }) {
                ExtendedLogger logger = LogManager.getContext(true).getLogger(clazz);
                before.put(logger, logger.getLevel());
                // hide logger
                Configurator.setLevel(logger, Level.OFF);
            }

            // second insert will get PRECONDITION_FAILED
            SparkException ex = Assert.assertThrows(SparkException.class,
                    () -> origin.write().format("ydb").options(ydbCreds)
                            .option("method", "INSERT")
                            .mode(SaveMode.Append).save("datetypes/insert_test")
            );
            Assert.assertTrue(ex.getCause() instanceof UnexpectedResultException);
            UnexpectedResultException reason = (UnexpectedResultException) ex.getCause();
            Assert.assertEquals(StatusCode.PRECONDITION_FAILED, reason.getStatus().getCode());
        } finally {
            // recover all loggers
            before.forEach((logger, level) -> Configurator.setLevel(logger, level));
            spark.read().format("ydb").options(ydbCreds)
                    .option("query", "DROP TABLE IF EXISTS `datetypes/insert_test`")
                    .load().count();
        }
    }

    @Test
    public void writeUpsertTest() {
        try {
            TestData data = new TestData(true);

            String createTable = "CREATE TABLE `datetypes/upsert_test`(" + data.toYqlColumns() + "PRIMARY KEY(id));";
            readYdb().option("query", createTable).load().count();

            Dataset<Row> origin = spark.createDataFrame(data.generateSet(1000, 2000), data.getSchema());
            origin.write().format("ydb").options(ydbCreds)
                    .option("method", "UPSERT")
                    .mode(SaveMode.Append).save("datetypes/upsert_test");

            TestData.assertEquals("upsert_test", 1000, origin, readYdb().load("datetypes/upsert_test").orderBy("id"));

            origin.write().format("ydb").options(ydbCreds)
                    .option("method", "UPSERT")
                    .mode(SaveMode.Append).save("datetypes/upsert_test");

            TestData.assertEquals("upsert_test", 1000, origin, readYdb().load("datetypes/upsert_test").orderBy("id"));
        } finally {
            spark.read().format("ydb").options(ydbCreds)
                    .option("query", "DROP TABLE IF EXISTS `datetypes/upsert_test`")
                    .load().count();
        }
    }

    @Test
    public void apacheArrowTest() {
        try {
            spark.read().format("ydb").options(ydbCreds)
                    .option("query", "DROP TABLE IF EXISTS `datetypes/arrow`")
                    .load().count();

            TestData data = new TestData(true);

            String createTable = "CREATE TABLE `datetypes/arrow`(" + data.toYqlColumns() + "PRIMARY KEY(id))"
                    + " WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=4);";
            readYdb().option("query", createTable).load().count();

            Dataset<Row> origin = spark.createDataFrame(data.generateSet(5000, 55000), data.getSchema());
            origin.write().format("ydb")
                    .options(ydbCreds)
                    .option("useApacheArrow", true)
                    .mode(SaveMode.Append).save("datetypes/arrow");

            TestData.assertEquals("arrow", 50000, origin,
                    readYdb().option("useApacheArrow", false).load("datetypes/arrow").orderBy("id"));
            TestData.assertEquals("arrow", 50000, origin,
                    readYdb().option("useApacheArrow", true).load("datetypes/arrow").orderBy("id"));
        } finally {
            spark.read().format("ydb").options(ydbCreds)
                    .option("query", "DROP TABLE IF EXISTS `datetypes/arrow`")
                    .load().count();
        }
    }
}
