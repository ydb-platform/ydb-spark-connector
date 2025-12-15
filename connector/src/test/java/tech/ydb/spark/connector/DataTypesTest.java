package tech.ydb.spark.connector;

import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
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

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("ydb-spark-predicates-test")
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

        YdbRegistry.closeAll();
    }

//    private static DataFrameReader readYdb() {
//        return spark.read().format("ydb").options(ydbCreds);
//    }

    @Test
    public void datetimeTest() {
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


        try {
            SparkException ex = Assert.assertThrows(SparkException.class,
                    () -> df1.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("datetypes/dates1")
            );
            Assert.assertTrue(ex.getCause() instanceof IllegalArgumentException);
            Assert.assertEquals("negative daysSinceEpoch: -3559", ex.getCause().getMessage());

            df1.write().format("ydb").options(ydbCreds).option("table.useSignedDatetypes", true).mode(SaveMode.Append)
                    .save("datetypes/dates2");
            Assert.assertEquals(3l, spark.read().format("ydb").options(ydbCreds).load("datetypes/dates2").count());
        } finally {
            spark.read().format("ydb").options(ydbCreds)
                    .option("query", "DROP TABLE IF EXISTS `datetypes/dates1`")
                    .load().count();
            spark.read().format("ydb").options(ydbCreds)
                    .option("query", "DROP TABLE IF EXISTS `datetypes/dates2`")
                    .load().count();
        }
    }


}
