package tech.ydb.spark.connector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
        DataFrameReader pushOn = readYdb().option("pushDownPredicate", "true");
        DataFrameReader pushOff = readYdb().option("pushDownPredicate", "false");

        assertDfEquals("Simple filter", 3,
                pushOn.load("row_table1").filter("step > 115").orderBy("hash"),
                pushOff.load("row_table1").filter("step > 115").orderBy("hash")
        );

        assertDfEquals("Multipli filters", 18,
                pushOn.load("row_table2").filter("sv = 97").filter("step > 100").orderBy("hash"),
                pushOff.load("row_table2").filter("sv = 97").filter("step > 100").orderBy("hash")
        );

        assertDfEquals("Column table filters", 192,
                pushOn.load("column_table").filter("step > 120").filter("sv < 200 OR sv > 300").orderBy("hash"),
                pushOff.load("column_table").filter("step > 120").filter("sv < 200 OR sv > 300").orderBy("hash")
        );
    }

    @Test
    public void pushDownLimitTest() {
        DataFrameReader pushOn = readYdb().option("pushDownLimit", "true");
        DataFrameReader pushOff = readYdb().option("pushDownLimit", "false");

        assertDfEquals("Small limit", 10,
                pushOn.load("row_table1").orderBy("hash").limit(10),
                pushOff.load("row_table1").orderBy("hash").limit(10)
        );

        assertDfEquals("Big limit", 3242,
                pushOn.load("row_table2").orderBy("hash").limit(3500),
                pushOff.load("row_table2").orderBy("hash").limit(3500)
        );

        assertDfEquals("Column table limit", 1000,
                pushOn.load("column_table").orderBy("hash").limit(1000),
                pushOff.load("column_table").orderBy("hash").limit(1000)
        );
    }

    @Test
    public void pushDownLikeTest() {
        DataFrameReader pushOn = readYdb().option("pushDownLimit", "true");
        DataFrameReader pushOff = readYdb().option("pushDownLimit", "false");

        assertDfEquals("Like startsWith", 14,
                pushOn.load("row_table1").filter("hash LIKE '00%'").orderBy("hash"),
                pushOff.load("row_table1").filter("hash LIKE '00%'").orderBy("hash")
        );

        assertDfEquals("Like endsWith", 15,
                pushOn.load("row_table2").filter("hash LIKE '%ff'").orderBy("hash"),
                pushOff.load("row_table2").filter("hash LIKE '%ff'").orderBy("hash")
        );

        assertDfEquals("Like contains", 9,
                pushOn.load("column_table").filter("hash LIKE '%dead%'").orderBy("hash"),
                pushOff.load("column_table").filter("hash LIKE '%dead%'").orderBy("hash")
        );
    }

    private void assertDfEquals(String message, int totalCount, Dataset<Row> ds1, Dataset<Row> ds2) {
        try {
            StructType schema = ds1.schema();
            Assert.assertEquals("[" + message + "] must have equals schemas", schema, ds2.schema());

            Iterator<Row> it1 = ds1.toLocalIterator();
            Iterator<Row> it2 = ds2.toLocalIterator();
            int count = 0;
            while (it1.hasNext()) {
                Assert.assertTrue("[" + message + "] ds1 has more rows then ds2", it2.hasNext());
                count++;

                Row r1 = it1.next();
                Row r2 = it2.next();

                for (int idx = 0; idx < schema.size(); idx += 1) {
                    String name = schema.fieldNames()[idx];
                    Assert.assertEquals("[" + message + "] row " + count + " column " + name + " must be equals",
                            r1.get(idx), r2.get(idx)
                    );
                }
            }

            Assert.assertFalse("[" + message + "] ds1 has less rows then ds2", it2.hasNext());
            Assert.assertEquals("[" + message + "] check rows count", totalCount, count);
        } catch (AssertionError ex) {
            ds1.show();
            ds2.show();
            throw ex;
        }
    }
}
