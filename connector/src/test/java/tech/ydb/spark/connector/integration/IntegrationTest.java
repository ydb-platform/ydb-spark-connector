package tech.ydb.spark.connector.integration;

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
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.spark.connector.impl.YdbConnector;
import tech.ydb.spark.connector.impl.YdbRegistry;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;

import static org.assertj.core.api.Assertions.assertThat;

public class IntegrationTest {

    public static final String CATALOG = "spark.sql.catalog.ydb1";
    public static final String TEST_TABLE = "test_table";

    public static final GenericContainer<?> YDB =
            new GenericContainer<>("cr.yandex/yc/yandex-docker-local-ydb:latest")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("localhost"))
            .withNetworkMode("host")
            .withEnv("GRPC_TLS_PORT", "2135")
            .withEnv("GRPC_PORT", "2136")
            .withEnv("MON_PORT", "8765")
            .withEnv("YDB_USE_IN_MEMORY_PDISKS", "true");

    static {
        YDB.start();
    }

    protected SparkSession spark;
    protected Map<String, String> options;
    protected YdbConnector connector;

    @Before
    public void prepare() {
        options = commonConfigs();
        spark = SparkSession.builder()
                .config(getSparkConf())
                .getOrCreate();
        connector = YdbRegistry.getOrCreate(options);
    }

    private SparkConf getSparkConf() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("test")
                .set(CATALOG, "tech.ydb.spark.connector.YdbCatalog");
        for (Map.Entry<String, String> one : options.entrySet()) {
            conf.set(CATALOG + "." + one.getKey(), one.getValue());
        }
        return conf;
    }

    private Map<String, String> commonConfigs() {
        HashMap<String, String> result = new HashMap<>();
        result.put("url", "grpc://127.0.0.1:2136?database=/local");
        result.put("auth.mode", "NONE");
        result.put("dbtable", TEST_TABLE);
        return result;
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
        assertThat(rows1).hasSize(1);
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
        assertThat(rows1).hasSize(3);

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
        assertThat(rows1).hasSize(1);
    }

    @Test
    public void testCreateBySparkSQL() {
        String original = TEST_TABLE + "_original";
        schemaQuery("create table " + original + "(id Uint64, value Text, PRIMARY KEY(id))").expectSuccess();
        dataQuery("upsert into " + original + "(id, value) values (1, 'asdf'), (2, 'zxcv'), (3, 'fghj')");

        spark.sql("create table ydb1." + TEST_TABLE + "(id bigint, value string) ").queryExecution();
        spark.sql("insert into ydb1." + TEST_TABLE + " select * from ydb1." + original).queryExecution();
        List<Row> rows = spark.sql("select * from ydb1." + TEST_TABLE)
                .collectAsList();
        assertThat(rows).hasSize(3);
    }

    @Test
    public void testCreateWithSelect() {
        String original = TEST_TABLE + "_original";
        schemaQuery("create table " + original + "(id Uint64, value Text, PRIMARY KEY(id))").expectSuccess();
        dataQuery("upsert into " + original + "(id, value) values (1, 'asdf'), (2, 'zxcv'), (3, 'fghj')");

        spark.sql("create table ydb1." + TEST_TABLE + " as select * from ydb1." + original)
                .queryExecution();
        List<Row> rows = spark.sql("select * from ydb1." + TEST_TABLE)
                .collectAsList();
        assertThat(rows).hasSize(3);
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

        List<Row> rows = spark.sql("SELECT * FROM ydb1.toster")
                .collectAsList();
        assertThat(rows).hasSize(2);
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
        connector.getRetryCtx().supplyStatus(session -> session.executeDataQuery(
                query,
                TxControl.serializableRw().setCommitTx(true),
                Params.empty())
                .thenApply(Result::getStatus))
                .join().expectSuccess();
    }

    private Status schemaQuery(String query) {
        return connector.getRetryCtx().supplyStatus(
                session -> session.executeSchemeQuery(query)).join();
    }

}
