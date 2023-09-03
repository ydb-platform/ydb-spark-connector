# Misc notes for YDB Spark Connector

[Reference 1](https://jaceklaskowski.github.io/spark-workshop/slides/spark-sql-Developing-Custom-Data-Source.html)

[Reference 2](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-data-source-api-v2.html)

[Cassandra Spark](https://github.com/datastax/spark-cassandra-connector)

https://levelup.gitconnected.com/easy-guide-to-create-a-custom-read-data-source-in-apache-spark-3-194afdc9627a
https://levelup.gitconnected.com/easy-guide-to-create-a-write-data-source-in-apache-spark-3-f7d1e5a93bdb
https://github.com/aamargajbhiye/big-data-projects/tree/master/Datasource%20spark3/src/main/java/com/bugdbug/customsource/jdbc

Spark Shell example config:

```bash
./bin/spark-shell --conf spark.sql.catalog.ydb=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb.url='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5' \
  --conf spark.sql.catalog.ydb.auth.mode=KEY \
  --conf spark.sql.catalog.ydb.auth.keyfile=/home/demo/Magic/key-ydb-sa1.json
```



```scala
spark.sql("SHOW NAMESPACES FROM ydb").show();
spark.sql("SHOW NAMESPACES FROM ydb.pgimp1").show();
spark.sql("SHOW TABLES FROM ydb").show();
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
spark.sql("SELECT * FROM ydb.toster").show();

spark.sql("SELECT COUNT(*) FROM ydb.test0_fhrw").show();
spark.sql("SELECT MIN(created_date) FROM ydb.test0_fhrw").show();
spark.sql("SELECT borough, MIN(created_date), MAX(created_date) FROM ydb.test0_fhrw GROUP BY borough ORDER BY borough").show();
spark.sql("SELECT city, COUNT(*) FROM ydb.pgimp1.public.fhrw WHERE unique_key<'2' GROUP BY city ORDER BY COUNT(*) DESC LIMIT 5").show(100, false);
spark.sql("SELECT city, COUNT(*) FROM ydb.pgimp1.public.fhrw WHERE unique_key<'2' AND unique_key>='1' GROUP BY city ORDER BY COUNT(*) DESC LIMIT 5").show(100, false);
```
