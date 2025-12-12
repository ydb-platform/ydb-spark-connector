package tech.ydb.spark.connector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.common.hash.Hashing;
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

import tech.ydb.test.junit4.YdbHelperRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class PredicatesTest {
    private final static String COLLATZ_SCHEME = ""
                + " sv Uint32 NOT NULL," // start value of sequence
                + " cv Uint32 NOT NULL," // current value of sequence
                + " idx Uint32 NOT NULL," // global index of row
                + " step Uint32 NOT NULL,"  // step of current sequence
                + " is_last Int32 NOT NULL, " // flag if the value is last in sequence, bool is not supported by CS yet
                + " hash Text NOT NULL,"; // sha256(sv, cv, idx, step)

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
        ydbCreds.put("table.autocreate", "false");

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

        initData();
    }

    @AfterClass
    public static void close() {
        if (spark != null) {
            dropTables();
            spark.close();
        }

        YdbRegistry.closeAll();
    }

    private static DataFrameReader readYdb() {
        return spark.read().format("ydb").options(ydbCreds);
    }

    private static void dropTables() {
        readYdb().option("query", ""
                + "DROP TABLE IF EXISTS row_table1;"
                + "DROP TABLE IF EXISTS row_table2;"
                + "DROP TABLE IF EXISTS column_table;"
        ).load().count();
    }

    private static void initData() {
        readYdb().option("query", "CREATE TABLE row_table1 ("
                + COLLATZ_SCHEME
                + "PRIMARY KEY(sv, cv)"
                + ") WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5, "
                + "  PARTITION_AT_KEYS = ((50), (70), (90, 2), (95, 10)) "
                + ")"
        ).load().count();

        readYdb().option("query", "CREATE TABLE row_table2 ("
                + COLLATZ_SCHEME
                + "PRIMARY KEY(hash)"
                + ") WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8, "
                + "  PARTITION_AT_KEYS = ('2', '4', '6', '8', 'a', 'c', 'e')  "
                + ")"
        ).load().count();

        readYdb().option("query", "CREATE TABLE column_table ("
                + COLLATZ_SCHEME
                + "PRIMARY KEY(hash)) WITH (STORE=COLUMN)"
        ).load().count();

        // 3242 records
        collatzSequence(100).write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("row_table1");
        collatzSequence(100).write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("row_table2");
        // 26643 records
        collatzSequence(500).write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("column_table");
    }

    private static Dataset<Row> collatzSequence(int size) {
        StructType schema = new StructType(new StructField[]{
            new StructField("sv", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("cv", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("idx", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("step", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("is_last", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("hash", DataTypes.StringType, false, Metadata.empty())
        });

        ArrayList<Row> rows = new ArrayList<>();
        rows.ensureCapacity(size);

        int sv = 1;
        int cs = 1;
        int step = 0;
        int idx = 0;
        while (sv <= size) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putInt(0, sv);
            bb.putInt(4, cs);
            bb.putInt(8, idx++);
            bb.putInt(12, step);
            String hash = Hashing.sha256().hashBytes(bb).toString();

            rows.add(new GenericRowWithSchema(new Object[]{ sv, cs, idx, step, cs == 1 ? 1 : 0, hash }, schema));

            if (cs == 1) {
                sv = sv + 1;
                cs = sv;
                step = 0;
            } else {
                step++;
                if (cs % 2 == 0) {
                    cs = cs / 2;
                } else {
                    cs = 3 * cs + 1;
                }
            }
        }

        return spark.createDataFrame(rows, schema);
    }

    @Test
    public void countTest() {
        long count1 = readYdb().load("row_table1").count();
        long count2 = readYdb().option("useReadTable", "true").load("row_table1").count();
        Assert.assertEquals(count1, count2);

        long count3 = readYdb().load("row_table2").count();
        long count4 = readYdb().option("useReadTable", "true").load("row_table2").count();
        Assert.assertEquals(count3, count4);

        long count5 = readYdb().load("column_table").count();
        Assert.assertEquals(3242, count1);
        Assert.assertEquals(3242, count3);
        Assert.assertEquals(26643, count5);
    }

    @Test
    public void customYqlTest() {
        // Select the row with maximal step == find the longest sequence.
        // 97 - is the longest sequence <= 100
        // 327 - is the longest sequence <= 500
        Row r1 = readYdb().option("query", "SELECT * FROM row_table1 ORDER BY step DESC LIMIT 1").load().first();
        Assert.assertEquals(Long.valueOf(97), r1.getAs("sv"));
        Row r2 = readYdb().option("query", "SELECT * FROM row_table2 ORDER BY step DESC LIMIT 1").load().first();
        Assert.assertEquals(Long.valueOf(97), r2.getAs("sv"));
        Row r3 = readYdb().option("query", "SELECT * FROM column_table ORDER BY step DESC LIMIT 1").load().first();
        Assert.assertEquals(Long.valueOf(327), r3.getAs("sv"));
    }

    @Test
    public void pushDownPredicateTest() {
        long c1 = readYdb().option("pushDownPredicate", "true").load("row_table1").filter("step > 115").count();
        long c2 = readYdb().option("pushDownPredicate", "false").load("row_table1").filter("step > 115").count();
        Assert.assertEquals(c1, c2);

        long c3 = readYdb().option("pushDownPredicate", "true").load("row_table2").filter("sv = 97").count();
        long c4 = readYdb().option("pushDownPredicate", "false").load("row_table2").filter("sv = 97").count();
        Assert.assertEquals(c3, c4);

        long c5 = readYdb().option("pushDownPredicate", "true").load("column_table")
                .filter("step > 120").filter("sv < 200 OR sv > 300").count();
        long c6 = readYdb().option("pushDownPredicate", "false").load("column_table")
                .filter("step > 120").filter("sv < 200 OR sv > 300").count();
        Assert.assertEquals(c5, c6);

        Assert.assertEquals(3, c1);
        Assert.assertEquals(3, c2);
        Assert.assertEquals(119, c3);
        Assert.assertEquals(119, c4);
        Assert.assertEquals(192, c5);
        Assert.assertEquals(192, c6);
    }

    @Test
    public void pushDownLimitTest() {
        long c1 = readYdb().option("pushDownLimit", "true").load("row_table1").limit(10).count();
        long c2 = readYdb().option("pushDownLimit", "false").load("row_table1").limit(10).count();
        Assert.assertEquals(c1, c2);

        long c3 = readYdb().option("pushDownLimit", "true").load("row_table2").limit(10).count();
        long c4 = readYdb().option("pushDownLimit", "false").load("row_table2").limit(10).count();
        Assert.assertEquals(c3, c4);

        long c5 = readYdb().option("pushDownLimit", "true").load("column_table").limit(100).count();
        long c6 = readYdb().option("pushDownLimit", "false").load("column_table").limit(100).count();
        Assert.assertEquals(c5, c6);

        Assert.assertEquals(10, c1);
        Assert.assertEquals(10, c2);
        Assert.assertEquals(10, c3);
        Assert.assertEquals(10, c4);
        Assert.assertEquals(100, c5);
        Assert.assertEquals(100, c6);
    }
}
