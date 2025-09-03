[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb-spark-connector/blob/main/LICENSE)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Frepo1.maven.org%2Fmaven2%2Ftech%2Fydb%2Fspark%2Fydb-spark-connector%2Fmaven-metadata.xml)](https://mvnrepository.com/artifact/tech.ydb.spark/ydb-spark-connector)
[![Build](https://img.shields.io/github/actions/workflow/status/ydb-platform/ydb-spark-connector/build.yaml)](https://github.com/ydb-platform/ydb-spark-connector/actions/workflows/build.yaml)
[![Codecov](https://img.shields.io/codecov/c/github/ydb-platform/ydb-spark-connector)](https://app.codecov.io/gh/ydb-platform/ydb-spark-connector)

# YDB Connector for Apache Spark


A high-performance connector that enables seamless integration between [YDB](https://ydb.tech/) and
[Apache Spark](https://spark.apache.org/), allowing you to read from and write to YDB tables directly from Spark
applications.

## Known limitations and future work

> [!WARNING]
> Column-oriented YDB tables are in the Preview mode.
> It's highly recommended to use the latest version of YDB to work with them.

1. The connector does not currently support consistent reading and transactional writes. Both features are planned to be
   implemented using the YDB's snapshots and its `CopyTables` and `RenameTables` APIs.
1. The connector may require significant memory on the Spark executor side to read large tables with the default
   settings. 2 GB or more memory per core is highly recommended.
1. Writing to YDB tables is supported only in 'Append' mode.
1. Reading and writing YDB tables containing columns of PostgreSQL-compatible types is not supported yet.
1. Predicate pushdown is not supported yet.
1. Handling of YDB's UInt64 data type is inefficient (conversion from and to the corresponding Spark type is performed
   through text representation).
1. Joining with large YDB tables may be inefficient, because key lookups are currently not supported.
1. When writing to YDB tables, there is no way to specify the primary key when explicit table creation is performed in
   the "error" and "overwrite" save modes. Random unique key is generated in that case and stored in the `_spark_key`
   column.

## Connector setup

The connector is deployed as a "fat" jar archive containing the code for its dependencies, including the GRPC libraries
and YDB Java SDK. That artifact is available in the Maven Global repository and can be used from it directly using the
`--packages` [option](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management)
(requires network connection).

```bash
./bin/spark-shell --packages tech.ydb.spark:ydb-spark-connector:2.0.1
```

Additionally, for greater convenience, the connector artifact can be downloaded from the
[releases section](https://github.com/ydb-platform/ydb-spark-connector/releases) and used locally by placing to the jars
folder in the Spark distribution or passing it as the --jars argument.
```bash
./bin/spark-shell --jars ydb-spark-connector-shaded-2.0.1.jar
```

## Connector usage

The connector can be used in one of two available styles:

* By defining one or more Spark "catalogs", with each catalog pointing to some YDB database.
* By configuring the data source options directly in the Spark job code for accessing a single table.

Both ways to use the connector require the mandatory option `url`, which specifies the target YDB database. The option
typically has the form of `<schema>://<endpoint>:<port>/<database>?<options>`. Here are a few examples:
* Local or remote Docker (anonymous authentication):<br>`grpc://localhost:2136/local`
* Self-hosted cluster:<br>`grpcs://<hostname>:2135/Root/testdb?secureConnectionCertificate=~/myca.cer`
* Connect with token to the cloud instance:<br>`grpcs://<hostname>:2135/path/to/database?tokenFile=~/my_token`
* Connect with service account to the cloud instance:<br>`grpcs://<hostname>:2135/path/to/database?saKeyFile=~/sa_key.json`

### Direct style usage

When configuring the "direct" style access to the particular YDB table to be read or written, the configuration settings
should be specified as shown in the examples below:

```Scala
// Read the data set from YDB table
val ydb_url = "grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g3o4minpkuh10pd2rj/etnfjib1gmua6mvvgdcl?saKeyFile=~/sa_key.json"

// Simple select
val df1 = spark.read.format("ydb").option("url", ydb_url).load("example_table");
df1.select("created_date", "complaint_type", "city").show(10, false)

// Copy data from Parquet file to YDB table
spark.read.format("parquet").load("file://test_file.parquet")
    .write.format("ydb").option("url", ydb_url).mode("append").save("table_from_parquet")

```

### Catalog style usage

YDB Spark catalog names, when defined, allow to access YDB tables in a way very similar to tables defined in the Spark's
Hive catalog. Each YDB-supported Spark catalog looks like a "database" from the Spark point of view.

When configuring the "catalog" style access to YDB database from the Spark job, the configuration properties should be
defined as described below:

* `spark.sql.catalog.<CatalogName>` should be set to `tech.ydb.spark.connector.YdbCatalog`, which configures the
`<CatalogName>` as Spark catalog for accessing YDB tables;
* `spark.sql.catalog.<CatalogName>.url` should be set to YDB database URL;
* `spark.sql.catalog.<CatalogName>.<Property>`, where `<Property>` is one of the supported configuration properties for
YDB connector (see the [reference in the YDB documentation](https://ydb.tech/docs/en/integrations/ingestion/spark)).

Spark SQL launch command example with `my_ydb` defined as the catalog name pointing to the local YDB docker container:

```bash
./bin/spark-sql \
    --conf spark.sql.catalog.my_ydb=tech.ydb.spark.connector.YdbCatalog \
    --conf spark.sql.catalog.my_ydb.url=grpc://localhost:2136/local
```

PySpark Shell launch command example with `ydb` defined as the catalog name pointing to the local YDB database installed
according to the [YDB Quickstart Instruction](https://ydb.tech/docs/en/getting_started/quickstart):

```bash
./bin/pyspark \
    --conf spark.sql.catalog.my_ydb=tech.ydb.spark.connector.YdbCatalog \
    --conf spark.sql.catalog.my_ydb.url='grpc://localhost:2136/Root/test'
```

The connector exposes YDB directories, tables and indexes as entries in the configured Spark catalog. Spark supports
recursive "namespaces", which works naturally with YDB's directories. Spark SQL uses "." (single dot) as the
namespace delimiter, so it should be used instead of YDB's "/" (forward slash) to define sub-directories and tables
within the YDB-enabled Spark catalog.

Here is an example of using Spark SQL to list YDB directories, tables, and table columns:

```
spark-sql> -- List root directories within the database
spark-sql> SHOW NAMESPACES FROM my_ydb;
cli
cs
jdbc
spark
Time taken: 1.632 seconds, Fetched 4 row(s)
spark-sql> -- List sub-directories within the specified directory
spark-sql (default)> SHOW NAMESPACES FROM my_ydb.cs;
cs.s1
Time taken: 0.044 seconds, Fetched 1 row(s)
spark-sql>
spark-sql> -- List the tables within the specified directory
spark-sql (default)> SHOW TABLES FROM my_ydb.jdbc;
csv_rows
Time taken: 0.046 seconds, Fetched 1 row(s)
spark-sql>
spark-sql> -- Describe the YDB table structure
spark-sql (default)> DESCRIBE TABLE my_ydb.jdbc.csv_rows;
hash                    string
event_time              string
event_type              string
product_id              decimal(22,0)
category_id             string
category_code           string
brand                   string
price                   decimal(38,10)
user_id                 decimal(22,0)
user_session            string

# Partitioning
Part 0                  bucket(16, hash)
Time taken: 0.038 seconds, Fetched 13 row(s)

spark-sql>
spark-sql> -- Run the simple Spark SQL query on top of YDB table
spark-sql (default)> SELECT hash, event_time FROM my_ydb.jdbc.csv_rows LIMIT 5;
0000001eff940518d62b116820e7e3dfe99104e9        2019-10-28 18:32:59 UTC
000000606b20d7764c37f3f2cadbf6d115a4216e        2019-10-01 04:22:55 UTC
0000008e8cb6da4fe8867cde4c0b4ce5ff204224        2019-10-03 02:47:31 UTC
0000009e1e4dbb3931f31ab38bfd9199159d5d40        2019-11-27 12:09:23 UTC
000000a256844a1d90f5116a221806900785c3a9        2019-11-15 14:31:32 UTC
Time taken: 0.125 seconds, Fetched 5 row(s)
```

## Logging configuration

Extra lines in `log4j2.properties`:

```java.properties
logger.ydb0.name = tech.ydb.spark
logger.ydb0.level = debug
logger.ydb1.name = tech.ydb
logger.ydb1.level = debug
logger.ydb2.name = tech.ydb.core.impl
logger.ydb2.level = warn
logger.ydb3.name = tech.ydb.table.impl
logger.ydb3.level = warn
logger.ydb4.name = tech.ydb.shaded
logger.ydb4.level = warn
```
