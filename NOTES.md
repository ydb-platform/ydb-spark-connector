# Misc notes for YDB Spark Connector

[Reference 1](https://jaceklaskowski.github.io/spark-workshop/slides/spark-sql-Developing-Custom-Data-Source.html)

[Reference 2](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-data-source-api-v2.html)

[Cassandra Spark](https://github.com/datastax/spark-cassandra-connector)

https://levelup.gitconnected.com/easy-guide-to-create-a-custom-read-data-source-in-apache-spark-3-194afdc9627a
https://levelup.gitconnected.com/easy-guide-to-create-a-write-data-source-in-apache-spark-3-f7d1e5a93bdb
https://github.com/aamargajbhiye/big-data-projects/tree/master/Datasource%20spark3/src/main/java/com/bugdbug/customsource/jdbc

Spark Shell example config:

```bash
./bin/spark-sql --conf spark.sql.catalog.ydb1=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb1.url='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g3o4minpkuh10pd2rj/etn5tp76hhfs5npr7j23' \
  --conf spark.sql.catalog.ydb1.auth.mode=KEY \
  --conf spark.sql.catalog.ydb1.auth.sakey.file=/Users/mzinal/Magic/key-mvz-ydb1.json \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007

./bin/spark-sql --conf spark.sql.catalog.ydb1=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb1.url='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g3o4minpkuh10pd2rj/etn5tp76hhfs5npr7j23' \
  --conf spark.sql.catalog.ydb1.auth.mode=KEY \
  --conf spark.sql.catalog.ydb1.auth.sakey.file=/home/zinal/Keys/ydb-sa1-key1.json \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007

cd /home/zinal/Software/spark-3.3.3-bin-hadoop3
./bin/spark-sql --conf spark.sql.catalog.ydb1=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb1.url='grpc://localhost:2136?database=/Root/test' \
  --conf spark.sql.catalog.ydb1.auth.mode=NONE

```


```sql
create schema ydb1.spark;
create table ydb1.spark.test1(a integer not null, b bigint, c varchar(100)) tblproperties('primary_key'='b,a');

CREATE TABLE ydb1.fhrw2 TBLPROPERTIES('primary_key'='h2,unique_key') AS SELECT hash(unique_key) AS h2, x.* FROM ydb1.fhrw0 x;
```


```scala
spark.sql("SHOW NAMESPACES FROM ydb1").show();
spark.sql("SHOW NAMESPACES FROM ydb1.pgimp1").show();
spark.sql("SHOW TABLES FROM ydb1").show();
```

```sql
CREATE TABLE toster(
  a Uint64 NOT NULL,
  b Uint32,
  c Int32,
  d Int64,
  e Text,
  f Bytes,
  g Timestamp,
  h Datetime,
  i Date,
  j Json,
  k JsonDocument,
  l Bool,
  m Uint8,
  n Float,
  o Double,
  p Decimal(22,9),
  PRIMARY KEY(a)
);

UPSERT INTO toster(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p) VALUES (
  1001,
  2002,
  3003,
  4004,
  "Text string"u,
  "Bytes string",
  Timestamp("2023-01-07T11:05:32.123456Z"),
  Datetime("2023-01-07T11:05:32Z"),
  Date("2023-01-07"),
  Json(@@{"x": 1, "y": "test"}@@),
  JsonDocument(@@{"x": 1, "y": "test"}@@),
  True,
  7,
  123.456f,
  123.456789,
  Decimal("123.456789", 22, 9)
), (
  10001,
  20002,
  30003,
  40004,
  "New Text string"u,
  "New Bytes string",
  Timestamp("2020-01-07T11:05:32.123456Z"),
  Datetime("2020-01-07T11:05:32Z"),
  Date("2020-01-07"),
  Json(@@{"x": 2, "y": "dust"}@@),
  JsonDocument(@@{"x": 2, "y": "dust"}@@),
  False,
  8,
  1023.456f,
  1023.456789,
  Decimal("1023.456789", 22, 9)
);
```

```scala
spark.sql("SELECT * FROM ydb1.toster").show();

spark.sql("SELECT COUNT(*) FROM ydb1.test0_fhrw").show();
spark.sql("SELECT MIN(created_date) FROM ydb1.test0_fhrw").show();
spark.sql("SELECT borough, MIN(created_date), MAX(created_date) FROM ydb1.test0_fhrw GROUP BY borough ORDER BY borough").show();
spark.sql("SELECT city, COUNT(*) FROM ydb1.pgimp1.public.fhrw WHERE unique_key<'2' GROUP BY city ORDER BY COUNT(*) DESC LIMIT 5").show(100, false);
spark.sql("SELECT city, COUNT(*) FROM ydb1.pgimp1.public.fhrw WHERE unique_key<'2' AND unique_key>='1' GROUP BY city ORDER BY COUNT(*) DESC LIMIT 5").show(100, false);
```

```bash
./bin/spark-shell --conf spark.sql.catalog.ydb1=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb1.url='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnd6mguvlul8qm4psvn' \
  --conf spark.sql.catalog.ydb1.auth.mode=KEY \
  --conf spark.sql.catalog.ydb1.date.as.string=true \
  --conf spark.sql.catalog.ydb1.auth.sakey.file=/home/zinal/Keys/mzinal-dp1.json
```

```scala
spark.sql("SELECT * FROM ydb1.test1_fhrw WHERE closed_date IS NOT NULL").show(10, false)

val df2 = spark.table("ydb1.`ix/test2_fhrw/ix1`")
df2.filter(df2("closed_date").gt(to_timestamp(lit("2010-02-01")))).show(10, false)

val df2 = (spark.read.format("ydb")
    .option("url", ydb_url)
    .option("auth.mode", "KEY")
    .option("auth.sakey.file", "/home/zinal/Keys/mzinal-dp1.json")
    .option("dbtable", "test2_fhrw/ix1/indexImplTable")
    .load)
df2.filter(df2("closed_date").gt(to_timestamp(lit("2010-02-02")))).show(10, false)
```

```scala

import org.apache.spark.sql.types._

val YDB_URL = "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5"
val YDB_KEYFILE = "/home/zinal/Keys/ydb-sa1-key1.json"

val YDB_KEYFILE = "/Users/mzinal/Magic/key-ydb-sa1.json"

val NUM_PART = 1000
val ROWS_PER_PART = 10000

val df1 = 1.to(NUM_PART).toDF("id_part").repartition(NUM_PART)
val df2 = df1.as[Int].mapPartitions(c => 1.to(ROWS_PER_PART).toIterator)
val df3 = df2.
  withColumn("spark_partition_id",spark_partition_id()).
  withColumn("a",(spark_partition_id() * ROWS_PER_PART) + col("value")).
  withColumn("b",col("a")+30).
  withColumn("c",col("a")+54321)
val df4 = df3.select("a", "b", "c")

df4.write.mode("append").format("ydb").
  option("url", YDB_URL).option("auth.mode", "KEY").option("auth.sakey.file", YDB_KEYFILE).
  option("method", "upsert").option("batchsize", "1000").option("dbtable", "mytable").save

val df0 = (spark.read.format("ydb").option("url", YDB_URL)
    .option("auth.mode", "KEY").option("auth.sakey.file", YDB_KEYFILE)
    .option("dbtable", "test0_fhrw")
    .load)
df0.show(10, false)

```

Log4j properties for debug:

```JavaProperties
logger.ydb0.name = tech.ydb.spark
logger.ydb0.level = debug
logger.ydb1.name = tech.ydb
logger.ydb1.level = info
logger.ydb2.name = tech.ydb.core.impl.discovery
logger.ydb2.level = info
```

Spark job master debugging:

```bash
./bin/spark-shell --conf spark.sql.catalog.ydb1=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb1.url='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5' \
  --conf spark.sql.catalog.ydb1.auth.mode=KEY \
  --conf spark.sql.catalog.ydb1.auth.sakey.file=/Users/mzinal/Magic/key-ydb-sa1.json \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007
```
