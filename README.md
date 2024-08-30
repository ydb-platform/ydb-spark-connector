# YDB connector for Apache Spark

> [!IMPORTANT]
> YDB Connector for Spark is at the early phase of development, and is currently not recommended for production workloads.

[YDB](https://ydb.tech) connector for [Apache Spark](https://spark.apache.org) can be used to integrate YDB data into Spark jobs. It supports reading and writing YDB tables, allowing for fast data access and ingestion.

For read operations, the connector supports basic filter pushdown using the primary keys and key prefixes. YDB indexes are exposed as Spark tables too, and can be used to query the data in a way similar to "normal" tables. YDB ReadTable API is used "under the covers".

For write operations, the connector uses the `UPSERT ... SELECT ...` statement to perform parallel data ingestion into the YDB tables. The connector can be configured to use the `REPLACE ... SELECT ...`, or YDB BulkUpsert API.

The connector has passed the basic tests with Apache Spark 3.3, 3.4 and 3.5, working on top of YDB 23.3.

## Known limitations and future work

1. The connector does not currently support consistent reading and transactional writes. Both features are planned to be implemented using the YDB's snapshots and its `CopyTables` and `RenameTables` APIs.
1. The connector may require significant memory on the Spark executor side to read large tables with the default settings. 4 GB or more memory per core is highly recommended.
1. Access to YDB columnar tables is not supported yet.
1. Predicate pushdown is limited to primary key, secondary index key (when accessing indexes), or their prefixes. It is specially important to better support pushdowns with YDB columnar tables.
1. Reading and writing YDB tables containing columns of PostgreSQL-compatible types is not supported yet.
1. Handling of YDB's UInt64 data type is inefficient (conversion from and to the corresponding Spark type is performed through text representation).
1. Joining with large YDB tables may be inefficient, because key lookups are currently not supported
1. Reading YDB system tables (those under the `.sys` directory) is currently not supported.

Those limitations are to be addressed in the future releases of the connector.

## Connector setup

The connector is deployed as a "fat" jar archive containing the code for its dependencies, including the GRPC libraries and YDB Java SDK. Commonly used dependencies are "shaded", e.g. put into the non-usual Java package to avoid the version conflicts with other libraries used in the Spark jobs.

Spark jobs using the connector should have the connector jar defined as a dependency, either explicitly (by putting it into the `--jars` argument of `spark-submit`) or implicitly (by putting into the system jars folder of the Spark installation).

The connector can be used in one of two available styles:

* by defining one or more Spark "catalogs", with each catalog pointing to some YDB database,
* by configuring the data source options directly in the Spark job code for accessing the single table.

YDB Spark catalog names, when defined, allow to access YDB tables in a way very similar to tables defined in the Spark's Hive catalog. Each YDB-supported Spark catalog looks like a "database" from the Spark point of view.

When configuring the "catalog" style access to YDB database from the Spark job, the configuration properties should be defined as described below:

* `spark.sql.catalog.<CatalogName>` should be set to `tech.ydb.spark.connector.YdbCatalog`, which configures the `<CatalogName>` as Spark catalog for accessing YDB tables;
* `spark.sql.catalog.<CatalogName>.url` should be set to YDB database URL, typically in the form of `grpcs://endpoint:port/?database=/Domain/dbname`;
* `spark.sql.catalog.<CatalogName>.<Property>`, where `<Property>` is one of the supported configuration properties for YDB connector (see the reference below).

> [!IMPORTANT]
> Do not use the literal value "ydb" for the catalog name ("ydb1" will work fine, for example). The attempt to use the YDB provider identifier - e.g. "ydb" - as the catalog name will cause calls like `spark.table("ydb.table_name")` to fail with the following error: "Unsupported data source type for direct query on files".

Spark Shell launch command example with `ydb1` defined as the catalog name pointing to the Serverless YDB database in Yandex Cloud:

```bash
export YDB_URL='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5'
export YDB_SAKEY=/home/demo/Magic/key-ydb-sa1.json
./bin/spark-shell --conf spark.sql.catalog.ydb1=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb1.url=${YDB_URL} \
  --conf spark.sql.catalog.ydb1.auth.mode=KEY \
  --conf spark.sql.catalog.ydb1.auth.sakey.file=${YDB_SAKEY}
```

PySpark Shell launch command example with `ydb1` defined as the catalog name pointing to the local YDB database installed according to the [YDB Quickstart Instruction](https://ydb.tech/docs/en/getting_started/quickstart):

```bash
./bin/pyspark --conf spark.sql.catalog.ydb1=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb1.url='grpc://localhost:2136?database=/Root/test'
```

When configuring the "direct" style access to the particular YDB table to be read or written, the configuration settings should be specified as shown in the examples below:

```Scala
// Read the data set from YDB table
val df1 = (spark.read.format("ydb")
    .option("url", "grpc://localhost:2136?database=/Root/test")
    .option("dbtable", "fhrw2")
    .load)
// Show the first 10 rows
df1.show(10, false)
// Write the data set to YDB table
```

```Python
# Read the data set from YDB table
df1 = spark.read.format("ydb") \
  .option("url", "grpc://localhost:2136?database=/Root/test") \
  .option("dbtable", "fhrw2") \
  .load()
# Show the first 10 rows
df1.show(10, False)
# Write the data set into the existing table
```

## Configuration reference

The following Spark configuration properties are supported by the YDB connector for Spark:

* `url` should be set to YDB database URL, typically in the form of `grpcs://endpoint:port/?database=/Domain/dbname`;
* `auth.mode` should specify the authentication mode setting, which can be one of:

    * `NONE` - anonymous access;
    * `STATIC` - static credentials, e.g. username and password;
    * `TOKEN` - explicit authentication token;
    * `META` - cloud virtual machine metadata authentication;
    * `KEY` - service account key file;
    * `ENV` - reading the authenticaton settings from the environment variables, as specified in the [documentation](https://ydb.tech/en/docs/reference/ydb-sdk/auth#env).

* `auth.{login,password}` - username and password for the STATIC authentication mode;
* `auth.sakey.file` - [authorized key file for Yandex Cloud](https://cloud.yandex.ru/docs/iam/concepts/authorization/key) for the KEY authentication mode;
* `auth.sakey.text` - alternative option to define the key as a literal property value for the KEY authentication mode;
* `auth.token` - explicit authentication token for the TOKEN authentication mode;
* `pool.size` - connection pool size, which should be bigger than the maximum number of concurrent Spark tasks per executor;
* `ca.file` - the file with PEM-encoded CA (Certificate Authority) certificates;
* `ca.text` - CA certificates bundle defined as a literal property value;
* `list.indexes` - true, if the indexes should be listed in the catalog along with regular YDB tables (default false);
* `date.as.string` - true to return dates and timestamps as strings, false otherwise (default false);
* `dbtable` - YDB table name to be read or written (for direct-style access), with YDB's `/` delimiters;
* `method` - YDB data ingestion method to be used, possible values (default `upsert`):

    * `upsert` - use the `UPSERT ... SELECT ...` batch YQL statements;
    * `replace` - use the `REPLACE ... SELECT ...` batch YQL statements;
    * `bulk` - use the BulkUpsert YDB API for data ingestion.

* `batchsize` - max batch rows to be ingested in a single portion, default 500. Use with care, typically should not exceed 1000;
* `primary_key` - list of comma-separated column names to define the YDB table's primary key (only supported for `CREATE TABLE` operations).

## Using the SQL statements with YDB catalog defined

The connector exposes the YDB directories, tables and indexes as entries in the Spark catalog configured. Spark supports recursive "namespaces", which works naturally with the YDB's directories. Spark SQL uses "." (single dot) as the namespace delimiter, so it should be used instead of YDB's "/" (forward slash) to define sub-directories and tables within the YDB-enabled Spark catalog.

Please see the example below on using Spark SQL to list YDB directories, tables, and table columns.

```sql
spark-sql> -- List root directories within the database
spark-sql> SHOW NAMESPACES FROM ydb1;
`demo-payments`
myschema1
mysql1
pgimp1
`python-examples`
zeppelin
`.sys`
spark-sql>
spark-sql> -- List sub-directories within the specified directory
spark-sql> SHOW NAMESPACES FROM ydb1.`python-examples`;
`python-examples`.basic
`python-examples`.jsondemo
`python-examples`.jsondemo1
`python-examples`.pagination
`python-examples`.secondary_indexes_builtin
`python-examples`.ttl
spark-sql>
spark-sql> -- List the tables within the specified directory
spark-sql> SHOW TABLES FROM ydb1.`python-examples`.`basic`;
episodes
seasons
series
spark-sql>
spark-sql> -- Describe the YDB table structure
spark-sql> DESCRIBE TABLE ydb1.`python-examples`.`basic`.episodes;
series_id           	bigint
season_id           	bigint
episode_id          	bigint
title               	string
air_date            	bigint

# Partitioning
Not partitioned
spark-sql>
spark-sql> -- Run the simple Spark SQL query on top of YDB table
spark-sql> SELECT * FROM ydb1.`python-examples`.`basic`.episodes LIMIT 5;
1	1	1	Yesterday's Jam	13182
1	1	2	Calamity Jen	13182
1	1	3	Fifty-Fifty	13189
1	1	4	The Red Door	13196
1	1	5	The Haunting of Bill Crouse'	13203
spark-sql>
spark-sql> -- Create the YDB table, specifying the primary key
spark-sql> CREATE TABLE ydb1.mytab1(a integer not null, b string, c timestamp) TBLPROPERTIES('primary_key'='a');
spark-sql>
spark-sql> -- Insert a row into the YDB table
spark-sql> INSERT INTO ydb1.mytab1(a,b,c) VALUES(1, 'One', CAST('2019-06-13 13:22:30.521' AS TIMESTAMP));
spark-sql>
spark-sql> -- Create a copy of the YDB table with extra column and a different primary key
spark-sql> CREATE TABLE ydb1.fhrw2 TBLPROPERTIES('primary_key'='h2') AS
         > SELECT sha2(unique_key, 256) AS h2, x.* FROM ydb1.fhrw0 x;
```

## Accessing YDB with Scala/Spark

Below there are some read operations using Scala:

```scala
// table access
spark.table("ydb1.test2_fhrw").select("created_date", "complaint_type", "city").show(10, false)

// index access - note the backticks and the naming format
spark.table("ydb1.`ix/test2_fhrw/ix1`").show(10, false)

// read from ydb, write to parquet files
spark.table("ydb1.test2_fhrw").write.parquet("s3a://mzinal-dproc1/tests/test2_fhrw");

val ydb_url = "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnd6mguvlul8qm4psvn"

// direct specification of connection properties
val df1 = (spark.read.format("ydb")
    .option("url", ydb_url)
    .option("auth.mode", "META")
    .option("table", "test2_fhrw")
    .load)
df1.select("created_date", "complaint_type", "city").show(10, false)

// same for index table access - note the "indexImplTable" name
val df2 = (spark.read.format("ydb")
    .option("url", ydb_url)
    .option("auth.mode", "META")
    .option("table", "test2_fhrw/ix1/indexImplTable")
    .load)
df2.filter(df2("closed_date").gt(to_timestamp(lit("2010-02-01")))).show(10, false)

// build some dataframe from the literal list of values
import spark.implicits._
import org.apache.spark.sql.Row
val someData = Seq(
  Row(4, 501, 401, 4001),
  Row(4, 502, 402, 4002),
  Row(4, 503, 403, 4003)
)
val someSchema = List(
  StructField("a_ref", IntegerType, true),
  StructField("id", IntegerType, true),
  StructField("q", IntegerType, true),
  StructField("z", IntegerType, true)
)
val someDF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  StructType(someSchema)
)
// writing data to the existing table available via "ydb1" catalog
someDF.write.mode("append").saveAsTable("ydb1.table3")

```

## Accessing YDB with Python/Spark

```python
# table access
spark.table("ydb1.fhrw0").select("created_date", "complaint_type", "city").show(10, False)

# index access - note the backticks and the naming format
spark.table("ydb1.`ix/fhrw0/ix1`").show(10, False)

# read from ydb, write to parquet files
spark.table("ydb1.fhrw0").write.parquet("file:///tmp/ydb-fhrw0")

```

## Logging configuration

Extra lines in `log4j2.properties`:

```
logger.ydb0.name = tech.ydb.spark
logger.ydb0.level = debug
logger.ydb1.name = tech.ydb
logger.ydb1.level = debug
logger.ydb2.name = tech.ydb.core.impl.discovery
logger.ydb2.level = info
logger.ydb3.name = tech.ydb.shaded
logger.ydb3.level = warn
```
