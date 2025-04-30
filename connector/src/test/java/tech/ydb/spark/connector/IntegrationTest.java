package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.test.junit4.YdbHelperRule;

public class IntegrationTest {
    @ClassRule
    public static final YdbHelperRule YDB = new YdbHelperRule();

    public static final String TEST_TABLE = "spark_test_table";

    private static GrpcTransport transport;
    private static TableClient tableClient;
    private static SessionRetryContext retryCtx;

    private static final Map<String, String> options = new HashMap<>();
    private static SparkSession spark;

    @BeforeClass
    public static void prepare() {
        options.put("url", (YDB.useTls() ? "grpcs" : "grpc") + "://" + YDB.endpoint() + YDB.database());
        if (YDB.authToken() != null) {
            options.put("auth.mode", "TOKEN");
            options.put("auth.token", YDB.authToken());
        } else {
            options.put("auth.mode", "NONE");
        }
        options.put("dbtable", TEST_TABLE);

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("ydb-spark-integration-test")
                .set("spark.ui.enabled", "false")
                .set("spark.sql.catalog.ydb", "tech.ydb.spark.connector.YdbCatalog");
        options.forEach((k, v) -> conf.set("spark.sql.catalog.ydb." + k, v));

        transport = YDB.createTransport();
        tableClient = TableClient.newClient(transport).build();
        retryCtx = SessionRetryContext.create(tableClient).build();

        spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    @AfterClass
    public static void closeAll() {
        if (spark != null) {
            spark.close();
        }
        if (tableClient != null) {
            tableClient.close();
        }
        if (transport != null) {
            transport.close();
        }
    }

    @After
    public void cleanup() {
        schemaQuery("drop table " + TEST_TABLE);
        schemaQuery("drop table " + TEST_TABLE + "_original");
        schemaQuery("drop table toster");
    }

    @Test
    public void testViaJavaApi() {
        Dataset<Row> dataFrame = sampleDataset();

        dataFrame.write()
                .format("ydb")
                .options(options)
                .mode(SaveMode.Overwrite)
                .save();

        List<Row> rows1 = spark.read()
                .format("ydb")
                .options(options)
                .schema(dataFrame.schema())
                .load()
                .collectAsList();

        Assert.assertEquals(1, rows1.size());
    }

    @Test
    public void testOverwrite() {
        schemaQuery("create table " + TEST_TABLE + "(id Uint64, value Text, PRIMARY KEY(id))").expectSuccess();
        dataQuery("upsert into " + TEST_TABLE + "(id, value) values (1, 'asdf'), (2, 'zxcv'), (3, 'fghj')");

        Dataset<Row> dataFrame = sampleDataset();

        List<Row> rows1 = spark.read()
                .format("ydb")
                .options(options)
                .schema(dataFrame.schema())
                .load()
                .collectAsList();
        Assert.assertEquals(3, rows1.size());

        dataFrame.write()
                .format("ydb")
                .options(options)
                .mode(SaveMode.Overwrite)
                .save();

        rows1 = spark.read()
                .format("ydb")
                .options(options)
                .schema(dataFrame.schema())
                .load()
                .collectAsList();
        Assert.assertEquals(1, rows1.size());
    }

    @Test
    public void testCreateBySparkSQL() {
        String original = TEST_TABLE + "_original";
        schemaQuery("create table " + original + "(id Uint64, value Text, PRIMARY KEY(id))").expectSuccess();
        dataQuery("upsert into " + original + "(id, value) values (1, 'asdf'), (2, 'zxcv'), (3, 'fghj')");

        spark.sql("create table ydb." + TEST_TABLE + "(id bigint, value string) ").queryExecution();
        spark.sql("insert into ydb." + TEST_TABLE + " select * from ydb." + original).queryExecution();
        List<Row> rows = spark.sql("select * from ydb." + TEST_TABLE)
                .collectAsList();

        Assert.assertEquals(3, rows.size());
    }

    @Test
    public void testCreateWithSelect() {
        String original = TEST_TABLE + "_original";
        schemaQuery("create table " + original + "(id Uint64, value Text, PRIMARY KEY(id))").expectSuccess();
        dataQuery("upsert into " + original + "(id, value) values (1, 'asdf'), (2, 'zxcv'), (3, 'fghj')");

        spark.sql("create table ydb." + TEST_TABLE + " as select * from ydb." + original)
                .queryExecution();
        List<Row> rows = spark.sql("select * from ydb." + TEST_TABLE)
                .collectAsList();

        Assert.assertEquals(3, rows.size());
    }

    @Test
    public void testCatalogAccess() {
        String createToster = "CREATE TABLE toster(\n"
                + "  a Uint64 NOT NULL,\n"
                + "  b Uint32,\n"
                + "  c Int32,\n"
                + "  d Int64,\n"
                + "  e Text,\n"
                + "  f Bytes,\n"
                + "  g Timestamp,\n"
                + "  h Datetime,\n"
                + "  i Date,\n"
                + "  j Json,\n"
                + "  k JsonDocument,\n"
                + "  l Bool,\n"
                + "  m Uint8,\n"
                + "  n Float,\n"
                + "  o Double,\n"
                + "  p Decimal(22,9),\n"
                + "  PRIMARY KEY(a)\n"
                + ")";

        schemaQuery(createToster).expectSuccess();

        String upsertToster = "UPSERT INTO toster(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p) VALUES (\n"
                + "  1001,\n"
                + "  2002,\n"
                + "  3003,\n"
                + "  4004,\n"
                + "  \"Text string\"u,\n"
                + "  \"Bytes string\",\n"
                + "  Timestamp(\"2023-01-07T11:05:32.123456Z\"),\n"
                + "  Datetime(\"2023-01-07T11:05:32Z\"),\n"
                + "  Date(\"2023-01-07\"),\n"
                + "  Json(@@{\"x\": 1, \"y\": \"test\"}@@),\n"
                + "  JsonDocument(@@{\"x\": 1, \"y\": \"test\"}@@),\n"
                + "  True,\n"
                + "  7,\n"
                + "  123.456f,\n"
                + "  123.456789,\n"
                + "  Decimal(\"123.456789\", 22, 9)\n"
                + "), (\n"
                + "  10001,\n"
                + "  20002,\n"
                + "  30003,\n"
                + "  40004,\n"
                + "  \"New Text string\"u,\n"
                + "  \"New Bytes string\",\n"
                + "  Timestamp(\"2020-01-07T11:05:32.123456Z\"),\n"
                + "  Datetime(\"2020-01-07T11:05:32Z\"),\n"
                + "  Date(\"2020-01-07\"),\n"
                + "  Json(@@{\"x\": 2, \"y\": \"dust\"}@@),\n"
                + "  JsonDocument(@@{\"x\": 2, \"y\": \"dust\"}@@),\n"
                + "  False,\n"
                + "  8,\n"
                + "  1023.456f,\n"
                + "  1023.456789,\n"
                + "  Decimal(\"1023.456789\", 22, 9)\n"
                + ")";

        dataQuery(upsertToster);

        List<Row> rows = spark.sql("SELECT * FROM ydb.toster")
                .collectAsList();

        Assert.assertEquals(2, rows.size());
    }

    private Dataset<Row> sampleDataset() {
        ArrayList<Row> rows = new ArrayList<>();
        StructType schema = new StructType(new StructField[]{
            new StructField("id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("value", DataTypes.StringType, false, Metadata.empty()),});
        rows.add(new GenericRowWithSchema(new Object[]{1L, "some value"}, schema));
        return spark.createDataFrame(rows, schema);
    }

    private void dataQuery(String query) {
        retryCtx.supplyResult(session -> session.executeDataQuery(query, TxControl.serializableRw()))
                .join().getStatus().expectSuccess();
    }

    private static Status schemaQuery(String query) {
        return retryCtx.supplyStatus(session -> session.executeSchemeQuery(query)).join();
    }
}
