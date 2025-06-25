package tech.ydb.spark.connector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

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
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import tech.ydb.spark.connector.impl.YdbExecutor;
import tech.ydb.test.junit4.YdbHelperRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PredicatesTest {
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
        if (ctx != null) {
            cleanTables(ctx.getExecutor());
            ctx.close();
        }
        ctx.close();
    }

    private static void prepareTables(YdbExecutor executor) {
        executor.executeSchemeQuery("CREATE TABLE row_table1 ("
                + " sv Uint32 NOT NULL,"
                + " cv Uint32 NOT NULL,"
                + " idx Uint32 NOT NULL,"
                + " step Uint32 NOT NULL, "
                + " is_last Int32 NOT NULL," // Bool is not supported by CS yet
                + " hash Text NOT NULL,"
                + " PRIMARY KEY(sv, cv)"
                + ") WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5, "
                + "  PARTITION_AT_KEYS = ((500), (700), (900, 2), (950, 10)) "
                + ")").join().expectSuccess("cannot create row_table1 table");
        executor.executeSchemeQuery("CREATE TABLE row_table2 ("
                + " sv Uint32 NOT NULL,"
                + " cv Uint32 NOT NULL,"
                + " idx Uint32 NOT NULL,"
                + " step Uint32 NOT NULL, "
                + " is_last Int32 NOT NULL," // Bool is not supported by CS yet
                + " hash Text NOT NULL,"
                + " PRIMARY KEY(hash)  "
                + ") WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8, "
                + "  PARTITION_AT_KEYS = ('2', '4', '6', '8', 'a', 'c', 'e') "
                + ")").join().expectSuccess("cannot create row_table2 table");
        executor.executeSchemeQuery("CREATE TABLE column_table ("
                + " sv Uint32 NOT NULL,"
                + " cv Uint32 NOT NULL,"
                + " idx Uint32 NOT NULL,"
                + " step Uint32 NOT NULL, "
                + " is_last Int32 NOT NULL," // Bool is not supported by CS yet
                + " hash Text NOT NULL,"
                + " PRIMARY KEY(hash)  "
                + ") WITH ("
                + "  STORE=COLUMN"
                + ")").join().expectSuccess("cannot create column_table table");
    }

    private static void cleanTables(YdbExecutor executor) {
        executor.executeSchemeQuery("DROP TABLE row_table1;").join();
        executor.executeSchemeQuery("DROP TABLE row_table2;").join();
        executor.executeSchemeQuery("DROP TABLE column_table;").join();
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
        for (int idx = 0; idx < size; idx++) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putInt(0, sv);
            bb.putInt(4, cs);
            bb.putInt(8, idx);
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

    private DataFrameReader readYdb() {
        return spark.read().format("ydb").option("url", ydbURL);
    }

    @Test
    public void test01_loadRowTable1() {
        collatzSequence(1000).write().format("ydb").option("url", ydbURL).mode(SaveMode.Append).save("row_table1");
        Assert.assertEquals(1000, spark.read().format("ydb").option("url", ydbURL).load("row_table1").count());
    }

    @Test
    public void test02_loadRowTable2() {
        collatzSequence(1000).write().format("ydb").option("url", ydbURL).mode(SaveMode.Append).save("row_table2");
        Assert.assertEquals(1000, spark.read().format("ydb").option("url", ydbURL).load("row_table2").count());
    }

    @Test
    public void test03_loadColumnTable() {
        collatzSequence(5000).write().format("ydb").option("url", ydbURL).mode(SaveMode.Append).save("column_table");
        Assert.assertEquals(5000, spark.read().format("ydb").option("url", ydbURL).load("column_table").count());
    }

    @Test
    public void test04_count() {
        long count1 = readYdb().load("row_table1").count();
        long count2 = readYdb().option("read.method", "READ_TABLE").load("row_table1").count();
        Assert.assertEquals(count1, count2);

        long count3 = readYdb().load("row_table2").count();
        long count4 = readYdb().option("read.method", "READ_TABLE").load("row_table2").count();
        Assert.assertEquals(count3, count4);

        long count5 = readYdb().load("column_table").count();
        Assert.assertEquals(1000, count1);
        Assert.assertEquals(1000, count3);
        Assert.assertEquals(5000, count5);
    }
}
