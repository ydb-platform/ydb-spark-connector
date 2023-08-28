# ydb-spark-ds
Experimental implementation of Spark Data Source for YDB

[Reference 1](https://jaceklaskowski.github.io/spark-workshop/slides/spark-sql-Developing-Custom-Data-Source.html)

[Reference 2](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-data-source-api-v2.html)

[Cassandra Spark](https://github.com/datastax/spark-cassandra-connector)


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

```scala
spark.sql("SELECT COUNT(*) FROM ydb.test0_fhrw").show();
spark.sql("SELECT MIN(created_date) FROM ydb.test0_fhrw").show();
spark.sql("SELECT borough, MIN(created_date), MAX(created_date) FROM ydb.test0_fhrw GROUP BY borough ORDER BY borough").show();
```
