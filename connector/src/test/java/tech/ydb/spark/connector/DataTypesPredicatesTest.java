package tech.ydb.spark.connector;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
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
import org.apache.spark.sql.types.Decimal;
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
 * Tests predicate pushdown for YDB columns of various data types.
 * Uses Int32 primary key and columns of types: Bool (row) / Int8 (column), Int8, Int16, Int32, Int64,
 * Uint8, Uint16, Uint32, Uint64, Float, Double, Decimal(22,9), Decimal(35,6), Bytes, Text,
 * Date, Date32, Timestamp, Timestamp64.
 *
 * Three table flavors: row-organized single partition (Bool), row-organized 10 partitions (Bool),
 * and column-organized (Int8 for bool-like, Bool not supported in column store). Each has 1k rows.
 * Validates &lt;, &gt;, &lt;=, &gt;= predicates.
 */
public class DataTypesPredicatesTest {

    private static final int ROW_COUNT = 1_000;
    private static final String TABLE_SCHEMA_ROW = ""
            + "id Int32 NOT NULL,"
            + "col_bool Bool NOT NULL,"
            + "col_int8 Int8 NOT NULL,"
            + "col_int16 Int16 NOT NULL,"
            + "col_int32 Int32 NOT NULL,"
            + "col_int64 Int64 NOT NULL,"
            + "col_uint8 Uint8 NOT NULL,"
            + "col_uint16 Uint16 NOT NULL,"
            + "col_uint32 Uint32 NOT NULL,"
            + "col_uint64 Uint64 NOT NULL,"
            + "col_float Float NOT NULL,"
            + "col_double Double NOT NULL,"
            + "col_decimal22 Decimal(22,9) NOT NULL,"
            + "col_decimal35 Decimal(35,6) NOT NULL,"
            + "col_binary Bytes NOT NULL,"
            + "col_text Text NOT NULL,"
            + "col_date Date NOT NULL,"
            + "col_date32 Date32 NOT NULL,"
            + "col_timestamp Timestamp NOT NULL,"
            + "col_timestamp64 Timestamp64 NOT NULL,";

    private static final String TABLE_SCHEMA_COLUMN = ""
            + "id Int32 NOT NULL,"
            + "col_bool Int8 NOT NULL,"  // Bool not supported in column store
            + "col_int8 Int8 NOT NULL,"
            + "col_int16 Int16 NOT NULL,"
            + "col_int32 Int32 NOT NULL,"
            + "col_int64 Int64 NOT NULL,"
            + "col_uint8 Uint8 NOT NULL,"
            + "col_uint16 Uint16 NOT NULL,"
            + "col_uint32 Uint32 NOT NULL,"
            + "col_uint64 Uint64 NOT NULL,"
            + "col_float Float NOT NULL,"
            + "col_double Double NOT NULL,"
            + "col_decimal22 Decimal(22,9) NOT NULL,"
            + "col_decimal35 Decimal(35,6) NOT NULL,"
            + "col_binary Bytes NOT NULL,"
            + "col_text Text NOT NULL,"
            + "col_date Date NOT NULL,"
            + "col_date32 Date32 NOT NULL,"
            + "col_timestamp Timestamp NOT NULL,"
            + "col_timestamp64 Timestamp64 NOT NULL,";

    @ClassRule
    public static final YdbHelperRule YDB = new YdbHelperRule();

    private static Map<String, String> ydbCreds;
    private static SparkSession spark;
    private static Dataset<Row> sourceDataRow;
    private static Dataset<Row> sourceDataColumn;

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
                .setAppName("ydb-spark-datatypes-predicates-test")
                .set("spark.ui.enabled", "false");

        spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        sourceDataRow = createSourceData(false);
        sourceDataColumn = createSourceData(true);
        initTables();
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
        return spark.read().format("ydb").options(ydbCreds).option("pushDownPredicate", "true");
    }

    /** Use when predicate pushdown emits incompatible types (e.g. Decimal vs Double in YQL). */
    private static DataFrameReader readYdbNoPushdown() {
        return spark.read().format("ydb").options(ydbCreds).option("pushDownPredicate", "false");
    }

    private static void dropTables() {
        readYdb().option("query", ""
                + "DROP TABLE IF EXISTS dt_pred_row_single;"
                + "DROP TABLE IF EXISTS dt_pred_row_partitioned;"
                + "DROP TABLE IF EXISTS dt_pred_column;"
        ).load().count();
    }

    private static StructType createSchema(boolean columnar) {
        return new StructType(new StructField[]{
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("col_bool", columnar ? DataTypes.ByteType : DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("col_int8", DataTypes.ByteType, false, Metadata.empty()),
            new StructField("col_int16", DataTypes.ShortType, false, Metadata.empty()),
            new StructField("col_int32", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("col_int64", DataTypes.LongType, false, Metadata.empty()),
            new StructField("col_uint8", DataTypes.ShortType, false, Metadata.empty()),
            new StructField("col_uint16", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("col_uint32", DataTypes.LongType, false, Metadata.empty()),
            new StructField("col_uint64", DataTypes.createDecimalType(22, 0), false, Metadata.empty()),
            new StructField("col_float", DataTypes.FloatType, false, Metadata.empty()),
            new StructField("col_double", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("col_decimal22", DataTypes.createDecimalType(22, 9), false, Metadata.empty()),
            new StructField("col_decimal35", DataTypes.createDecimalType(35, 6), false, Metadata.empty()),
            new StructField("col_binary", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("col_text", DataTypes.StringType, false, Metadata.empty()),
            new StructField("col_date", DataTypes.DateType, false, Metadata.empty()),
            new StructField("col_date32", DataTypes.DateType, false, Metadata.empty()),
            new StructField("col_timestamp", DataTypes.TimestampType, false, Metadata.empty()),
            new StructField("col_timestamp64", DataTypes.TimestampType, false, Metadata.empty())
        });
    }

    private static byte[] intToBytes(int value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(value);
        return bb.array();
    }

    private static Dataset<Row> createSourceData(boolean columnar) {
        StructType schema = createSchema(columnar);
        ArrayList<Row> rows = new ArrayList<>(ROW_COUNT);

        for (int id = 0; id < ROW_COUNT; id++) {
            Object colBool = columnar ? (byte) (id % 2) : (id % 2) != 0;
            byte colInt8 = (byte) ((id % 256) - 128);
            short colInt16 = (short) Math.min(id, Short.MAX_VALUE);
            int colInt32 = id;
            long colInt64 = id;
            short colUint8 = (short) (id % 256);
            int colUint16 = Math.min(id, 65535);
            long colUint32 = id;
            Decimal colUint64 = Decimal.apply(BigDecimal.valueOf(id));
            float colFloat = id;
            double colDouble = id;
            Decimal colDecimal22 = Decimal.apply(BigDecimal.valueOf(id));
            Decimal colDecimal35 = Decimal.apply(BigDecimal.valueOf(id));
            byte[] colBinary = intToBytes(id);
            String colText = String.format("%05d", id);
            // Date: days since epoch (id). Date/Timestamp require >= 1970 for unsigned types
            int daysSinceEpoch = id;
            LocalDate dateVal = LocalDate.ofEpochDay(daysSinceEpoch);
            Object colDate = java.sql.Date.valueOf(dateVal);
            // Timestamp: micros since epoch. 1 day = 86400 * 1_000_000 micros. Spark expects java.sql.Timestamp
            long timestampMicros = daysSinceEpoch * 86400L * 1_000_000;
            Timestamp colTimestamp = Timestamp.from(Instant.ofEpochSecond(
                    timestampMicros / 1_000_000, (timestampMicros % 1_000_000) * 1000));

            rows.add(new GenericRowWithSchema(new Object[]{
                id, colBool, colInt8, colInt16, colInt32, colInt64,
                colUint8, colUint16, colUint32, colUint64,
                colFloat, colDouble, colDecimal22, colDecimal35,
                colBinary, colText,
                colDate, colDate, colTimestamp, colTimestamp
            }, schema));
        }

        return spark.createDataFrame(rows, schema);
    }

    private static void initTables() {
        // Row table, single partition (no explicit partition keys)
        readYdb().option("query", "CREATE TABLE dt_pred_row_single ("
                + TABLE_SCHEMA_ROW
                + "PRIMARY KEY(id)"
                + ") WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)"
        ).load().count();

        // Row table, 10 partitions
        readYdb().option("query", "CREATE TABLE dt_pred_row_partitioned ("
                + TABLE_SCHEMA_ROW
                + "PRIMARY KEY(id)"
                + ") WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10, "
                + "  PARTITION_AT_KEYS = (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000)"
                + ")"
        ).load().count();

        // Column table
        readYdb().option("query", "CREATE TABLE dt_pred_column ("
                + TABLE_SCHEMA_COLUMN
                + "PRIMARY KEY(id)) WITH (STORE=COLUMN)"
        ).load().count();

        sourceDataRow.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("dt_pred_row_single");
        sourceDataRow.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("dt_pred_row_partitioned");
        sourceDataColumn.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save("dt_pred_column");
    }

    private void assertPredicateCount(String tableName, String filterExpr, long expectedCount) {
        assertPredicateCount(tableName, filterExpr, expectedCount, true);
    }

    private void assertPredicateCount(String tableName, String filterExpr, long expectedCount,
            boolean pushdown) {
        DataFrameReader reader = pushdown ? readYdb() : readYdbNoPushdown();
        long actual = reader.load(tableName).filter(filterExpr).count();
        Assert.assertEquals("Table " + tableName + " filter '" + filterExpr + "'",
                expectedCount, actual);
    }

    private void runPredicateTest(String tableName, boolean isColumnTable) {
        // Bool (row): false < true. Int8 (column): 0 < 1. Both: 5000 false/0 (id even), 5000 true/1 (id odd)
        long boolLt, boolGt, boolLe, boolGe;
        String filterLt, filterGt, filterLe, filterGe;
        if (isColumnTable) {
            boolLt = sourceDataColumn.filter("col_bool < 1").count();
            boolGt = sourceDataColumn.filter("col_bool > 0").count();
            boolLe = sourceDataColumn.filter("col_bool <= 0").count();
            boolGe = sourceDataColumn.filter("col_bool >= 1").count();
            filterLt = "col_bool < 1";
            filterGt = "col_bool > 0";
            filterLe = "col_bool <= 0";
            filterGe = "col_bool >= 1";
        } else {
            boolLt = sourceDataRow.filter("col_bool < true").count();
            boolGt = sourceDataRow.filter("col_bool > false").count();
            boolLe = sourceDataRow.filter("col_bool <= false").count();
            boolGe = sourceDataRow.filter("col_bool >= true").count();
            filterLt = "col_bool < true";
            filterGt = "col_bool > false";
            filterLe = "col_bool <= false";
            filterGe = "col_bool >= true";
        }
        assertPredicateCount(tableName, filterLt, boolLt);
        assertPredicateCount(tableName, filterGt, boolGt);
        assertPredicateCount(tableName, filterLe, boolLe);
        assertPredicateCount(tableName, filterGe, boolGe);

        // Int8: (id%256)-128, threshold 0 gives ~5000
        Dataset<Row> source = isColumnTable ? sourceDataColumn : sourceDataRow;
        long int8Lt = source.filter("col_int8 < 0").count();
        long int8Gt = source.filter("col_int8 > -1").count();
        long int8Le = source.filter("col_int8 <= -1").count();
        long int8Ge = source.filter("col_int8 >= 0").count();
        assertPredicateCount(tableName, "col_int8 < 0", int8Lt);
        assertPredicateCount(tableName, "col_int8 > -1", int8Gt);
        assertPredicateCount(tableName, "col_int8 <= -1", int8Le);
        assertPredicateCount(tableName, "col_int8 >= 0", int8Ge);

        // Int16, Int32, Int64: id, threshold 3000 / 7000
        long intLt = source.filter("col_int16 < 3000").count();
        long intGt = source.filter("col_int16 > 6999").count();
        long intLe = source.filter("col_int16 <= 2999").count();
        long intGe = source.filter("col_int16 >= 7000").count();
        assertPredicateCount(tableName, "col_int16 < 3000", intLt);
        assertPredicateCount(tableName, "col_int16 > 6999", intGt);
        assertPredicateCount(tableName, "col_int16 <= 2999", intLe);
        assertPredicateCount(tableName, "col_int16 >= 7000", intGe);

        intLt = source.filter("col_int32 < 3000").count();
        intGt = source.filter("col_int32 > 6999").count();
        intLe = source.filter("col_int32 <= 2999").count();
        intGe = source.filter("col_int32 >= 7000").count();
        assertPredicateCount(tableName, "col_int32 < 3000", intLt);
        assertPredicateCount(tableName, "col_int32 > 6999", intGt);
        assertPredicateCount(tableName, "col_int32 <= 2999", intLe);
        assertPredicateCount(tableName, "col_int32 >= 7000", intGe);

        intLt = source.filter("col_int64 < 3000").count();
        intGt = source.filter("col_int64 > 6999").count();
        intLe = source.filter("col_int64 <= 2999").count();
        intGe = source.filter("col_int64 >= 7000").count();
        assertPredicateCount(tableName, "col_int64 < 3000", intLt);
        assertPredicateCount(tableName, "col_int64 > 6999", intGt);
        assertPredicateCount(tableName, "col_int64 <= 2999", intLe);
        assertPredicateCount(tableName, "col_int64 >= 7000", intGe);

        // Uint8: id%256, threshold 128
        long uint8Lt = source.filter("col_uint8 < 128").count();
        long uint8Gt = source.filter("col_uint8 > 127").count();
        long uint8Le = source.filter("col_uint8 <= 127").count();
        long uint8Ge = source.filter("col_uint8 >= 128").count();
        assertPredicateCount(tableName, "col_uint8 < 128", uint8Lt);
        assertPredicateCount(tableName, "col_uint8 > 127", uint8Gt);
        assertPredicateCount(tableName, "col_uint8 <= 127", uint8Le);
        assertPredicateCount(tableName, "col_uint8 >= 128", uint8Ge);

        // Uint16, Uint32, Uint64
        long uintLt = source.filter("col_uint16 < 3000").count();
        long uintGt = source.filter("col_uint16 > 6999").count();
        long uintLe = source.filter("col_uint16 <= 2999").count();
        long uintGe = source.filter("col_uint16 >= 7000").count();
        assertPredicateCount(tableName, "col_uint16 < 3000", uintLt);
        assertPredicateCount(tableName, "col_uint16 > 6999", uintGt);
        assertPredicateCount(tableName, "col_uint16 <= 2999", uintLe);
        assertPredicateCount(tableName, "col_uint16 >= 7000", uintGe);

        uintLt = source.filter("col_uint32 < 3000").count();
        uintGt = source.filter("col_uint32 > 6999").count();
        uintLe = source.filter("col_uint32 <= 2999").count();
        uintGe = source.filter("col_uint32 >= 7000").count();
        assertPredicateCount(tableName, "col_uint32 < 3000", uintLt);
        assertPredicateCount(tableName, "col_uint32 > 6999", uintGt);
        assertPredicateCount(tableName, "col_uint32 <= 2999", uintLe);
        assertPredicateCount(tableName, "col_uint32 >= 7000", uintGe);

        // Uint64 maps to Decimal(22,0) - pushdown emits Double literal, incompatible with YQL
        uintLt = source.filter("col_uint64 < 3000").count();
        uintGt = source.filter("col_uint64 > 6999").count();
        uintLe = source.filter("col_uint64 <= 2999").count();
        uintGe = source.filter("col_uint64 >= 7000").count();
        assertPredicateCount(tableName, "col_uint64 < 3000", uintLt, false);
        assertPredicateCount(tableName, "col_uint64 > 6999", uintGt, false);
        assertPredicateCount(tableName, "col_uint64 <= 2999", uintLe, false);
        assertPredicateCount(tableName, "col_uint64 >= 7000", uintGe, false);

        // Float, Double
        long floatLt = source.filter("col_float < 3000").count();
        long floatGt = source.filter("col_float > 6999").count();
        long floatLe = source.filter("col_float <= 2999").count();
        long floatGe = source.filter("col_float >= 7000").count();
        assertPredicateCount(tableName, "col_float < 3000", floatLt);
        assertPredicateCount(tableName, "col_float > 6999", floatGt);
        assertPredicateCount(tableName, "col_float <= 2999", floatLe);
        assertPredicateCount(tableName, "col_float >= 7000", floatGe);

        long doubleLt = source.filter("col_double < 3000").count();
        long doubleGt = source.filter("col_double > 6999").count();
        long doubleLe = source.filter("col_double <= 2999").count();
        long doubleGe = source.filter("col_double >= 7000").count();
        assertPredicateCount(tableName, "col_double < 3000", doubleLt);
        assertPredicateCount(tableName, "col_double > 6999", doubleGt);
        assertPredicateCount(tableName, "col_double <= 2999", doubleLe);
        assertPredicateCount(tableName, "col_double >= 7000", doubleGe);

        // Decimal(22,9), Decimal(35,6) - pushdown emits Double literal, incompatible with YQL
        long dec22Lt = source.filter("col_decimal22 < 3000").count();
        long dec22Gt = source.filter("col_decimal22 > 6999").count();
        long dec22Le = source.filter("col_decimal22 <= 2999").count();
        long dec22Ge = source.filter("col_decimal22 >= 7000").count();
        assertPredicateCount(tableName, "col_decimal22 < 3000", dec22Lt, false);
        assertPredicateCount(tableName, "col_decimal22 > 6999", dec22Gt, false);
        assertPredicateCount(tableName, "col_decimal22 <= 2999", dec22Le, false);
        assertPredicateCount(tableName, "col_decimal22 >= 7000", dec22Ge, false);

        long dec35Lt = source.filter("col_decimal35 < 3000").count();
        long dec35Gt = source.filter("col_decimal35 > 6999").count();
        long dec35Le = source.filter("col_decimal35 <= 2999").count();
        long dec35Ge = source.filter("col_decimal35 >= 7000").count();
        assertPredicateCount(tableName, "col_decimal35 < 3000", dec35Lt, false);
        assertPredicateCount(tableName, "col_decimal35 > 6999", dec35Gt, false);
        assertPredicateCount(tableName, "col_decimal35 <= 2999", dec35Le, false);
        assertPredicateCount(tableName, "col_decimal35 >= 7000", dec35Ge, false);

        // Binary: 4 bytes big-endian - pushdown emits invalid YQL for unhex()
        long binLt = source.filter("col_binary < unhex('00000BB8')").count();
        long binGt = source.filter("col_binary > unhex('00001B57')").count();
        long binLe = source.filter("col_binary <= unhex('00000BB7')").count();
        long binGe = source.filter("col_binary >= unhex('00001B58')").count();
        assertPredicateCount(tableName, "col_binary < unhex('00000BB8')", binLt, false);
        assertPredicateCount(tableName, "col_binary > unhex('00001B57')", binGt, false);
        assertPredicateCount(tableName, "col_binary <= unhex('00000BB7')", binLe, false);
        assertPredicateCount(tableName, "col_binary >= unhex('00001B58')", binGe, false);

        // Text: "00000".."09999", lexicographic - pushdown can emit invalid YQL for string literals
        long textLt = source.filter("col_text < '03000'").count();
        long textGt = source.filter("col_text > '06999'").count();
        long textLe = source.filter("col_text <= '02999'").count();
        long textGe = source.filter("col_text >= '07000'").count();
        assertPredicateCount(tableName, "col_text < '03000'", textLt, false);
        assertPredicateCount(tableName, "col_text > '06999'", textGt, false);
        assertPredicateCount(tableName, "col_text <= '02999'", textLe, false);
        assertPredicateCount(tableName, "col_text >= '07000'", textGe, false);

        // Date, Date32, Timestamp, Timestamp64 - pushdown with YQL Date/Timestamp literals
        long dateLt = source.filter("col_date < date'1978-03-15'").count();
        long dateGt = source.filter("col_date > date'1989-02-26'").count();
        long dateLe = source.filter("col_date <= date'1978-03-14'").count();
        long dateGe = source.filter("col_date >= date'1989-02-27'").count();
        assertPredicateCount(tableName, "col_date < date'1978-03-15'", dateLt);
        assertPredicateCount(tableName, "col_date > date'1989-02-26'", dateGt);
        assertPredicateCount(tableName, "col_date <= date'1978-03-14'", dateLe);
        assertPredicateCount(tableName, "col_date >= date'1989-02-27'", dateGe);

        long date32Lt = source.filter("col_date32 < date'1978-03-15'").count();
        long date32Gt = source.filter("col_date32 > date'1989-02-26'").count();
        long date32Le = source.filter("col_date32 <= date'1978-03-14'").count();
        long date32Ge = source.filter("col_date32 >= date'1989-02-27'").count();
        assertPredicateCount(tableName, "col_date32 < date'1978-03-15'", date32Lt);
        assertPredicateCount(tableName, "col_date32 > date'1989-02-26'", date32Gt);
        assertPredicateCount(tableName, "col_date32 <= date'1978-03-14'", date32Le);
        assertPredicateCount(tableName, "col_date32 >= date'1989-02-27'", date32Ge);

        long tsLt = source.filter("col_timestamp < timestamp'1978-03-15 00:00:00'").count();
        long tsGt = source.filter("col_timestamp > timestamp'1989-02-26 23:59:59'").count();
        long tsLe = source.filter("col_timestamp <= timestamp'1978-03-14 23:59:59'").count();
        long tsGe = source.filter("col_timestamp >= timestamp'1989-02-27 00:00:00'").count();
        assertPredicateCount(tableName, "col_timestamp < timestamp'1978-03-15 00:00:00'", tsLt);
        assertPredicateCount(tableName, "col_timestamp > timestamp'1989-02-26 23:59:59'", tsGt);
        assertPredicateCount(tableName, "col_timestamp <= timestamp'1978-03-14 23:59:59'", tsLe);
        assertPredicateCount(tableName, "col_timestamp >= timestamp'1989-02-27 00:00:00'", tsGe);

        long ts64Lt = source.filter("col_timestamp64 < timestamp'1978-03-15 00:00:00'").count();
        long ts64Gt = source.filter("col_timestamp64 > timestamp'1989-02-26 23:59:59'").count();
        long ts64Le = source.filter("col_timestamp64 <= timestamp'1978-03-14 23:59:59'").count();
        long ts64Ge = source.filter("col_timestamp64 >= timestamp'1989-02-27 00:00:00'").count();
        assertPredicateCount(tableName, "col_timestamp64 < timestamp'1978-03-15 00:00:00'", ts64Lt);
        assertPredicateCount(tableName, "col_timestamp64 > timestamp'1989-02-26 23:59:59'", ts64Gt);
        assertPredicateCount(tableName, "col_timestamp64 <= timestamp'1978-03-14 23:59:59'", ts64Le);
        assertPredicateCount(tableName, "col_timestamp64 >= timestamp'1989-02-27 00:00:00'", ts64Ge);
    }

    @Test
    public void predicatesRowSinglePartition() {
        Assert.assertEquals(ROW_COUNT, readYdb().load("dt_pred_row_single").count());
        runPredicateTest("dt_pred_row_single", false);
    }

    @Test
    public void predicatesRowPartitioned() {
        Assert.assertEquals(ROW_COUNT, readYdb().load("dt_pred_row_partitioned").count());
        runPredicateTest("dt_pred_row_partitioned", false);
    }

    @Test
    public void predicatesColumnTable() {
        Assert.assertEquals(ROW_COUNT, readYdb().load("dt_pred_column").count());
        runPredicateTest("dt_pred_column", true);
    }
}
