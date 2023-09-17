# Experimental Apache Spark connector for YDB

Experimental implementation of [Apache Spark](https://spark.apache.org) connector for [YDB](https://ydb.tech).

The connector uses YDB ReadTable API under the covers, and supports basic filters pushdown using the primary keys. YDB indexes are exposed as Spark tables too, and can be used to query the data in a way similar to "normal" tables.

## Configuration

The connector is deployed as a "fat" jar archive containing the code for its dependencies, including the GRPC libraries and YDB Java SDK. Commonly used dependencies are "shaded", e.g. put into the non-usual Java package to avoid the version conflicts with other libraries used in the Spark jobs.

Spark jobs using the connector should have its jar defined as a dependency, either explicitly (by putting it into the `--jars` argument of `spark-submit`) or implicitly (by putting into the system jars folder of the Spark installation).

The following Spark configuration parameters must be defined to use the Spark connector for YDB:

* `spark.sql.catalog.<CatalogName>` should be set to `tech.ydb.spark.connector.YdbCatalog`, which configures the `<CatalogName>` as Spark catalog for accessing YDB tables;
* `spark.sql.catalog.<CatalogName>.url` should be set to YDB database URL, typically in the form of `grpcs://endpoint:port/?database=/Domain/dbname`;
* `spark.sql.catalog.<CatalogName>.auth.mode` should specify the authentication mode setting, which can be one of:
    * `NONE` - anonymous access;
    * `STATIC` - static credentials, e.g. username and password;
    * `TOKEN` - explicit authentication token;
    * `META` - cloud virtual machine metadata authentication;
    * `KEY` - service account key file;
    * `ENV` - reading the authenticaton settings from the environment variables, as specified in the [documentation](https://ydb.tech/en/docs/reference/ydb-sdk/auth#env).
* `spark.sql.catalog.<CatalogName>.auth.{login,password}` - username and password for the STATIC authentication mode;
* `spark.sql.catalog.<CatalogName>.auth.keyfile` - [authorized key file for Yandex Cloud](https://cloud.yandex.ru/docs/iam/concepts/authorization/key) for the KEY authentication mode;
* `spark.sql.catalog.<CatalogName>.auth.token` - explicit authentication token for the TOKEN authentication mode;
* `spark.sql.catalog.<CatalogName>.pool.size` - connection pool size, which should be bigger than the maximum number of concurrent Spark tasks per executor.

Below is the example of running the interactive Spark shell, in the Scala mode, with the necessary configuration options:

```bash
./bin/spark-shell --conf spark.sql.catalog.ydb=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb.url='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5' \
  --conf spark.sql.catalog.ydb.auth.mode=KEY \
  --conf spark.sql.catalog.ydb.auth.keyfile=/home/demo/Magic/key-ydb-sa1.json
```

Using both Delta Lake and YDB connector to run interactive Spark SQL session:

```bash
spark-sql --conf spark.sql.catalog.ydb=tech.ydb.spark.connector.YdbCatalog \
  --conf spark.sql.catalog.ydb.url='grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gfvslmokutuvt2g019/etnd6mguvlul8qm4psvn' \
  --conf spark.sql.catalog.ydb.auth.mode=META \
  --jars s3a://mzinal-dproc1/jars/yc-delta23-multi-dp21-1.1-fatjar.jar,s3a://mzinal-dproc1/jars/ydb-spark-connector-1.0-SNAPSHOT.jar
```

## Supported operations

The connector exposes the YDB directories, tables and indexes as entries in the Spark catalog configured. Spark supports recursive "namespaces", which works naturally with the YDB's directories. Spark SQL uses "." (single dot) as the namespace delimiter, so it should be used instead of YDB's "/" (forward slash) to define sub-directories and tables within the YDB-enabled Spark catalog.

Please see the example below on using Spark SQL to list YDB directories, tables, and table columns.

```sql
spark-sql> -- List root directories within the database
spark-sql> SHOW NAMESPACES FROM ydb;
`demo-payments`
myschema1
mysql1
pgimp1
`python-examples`
zeppelin
`.sys`
spark-sql> -- List sub-directories within the specified directory
spark-sql> SHOW NAMESPACES FROM ydb.`python-examples`;
`python-examples`.basic
`python-examples`.jsondemo
`python-examples`.jsondemo1
`python-examples`.pagination
`python-examples`.secondary_indexes_builtin
`python-examples`.ttl
spark-sql> -- List the tables within the specified directory
spark-sql> SHOW TABLES FROM ydb.`python-examples`.`basic`;
episodes
seasons
series
spark-sql> -- Describe the YDB table structure
spark-sql> DESCRIBE TABLE ydb.`python-examples`.`basic`.episodes;
series_id           	bigint              	                    
season_id           	bigint              	                    
episode_id          	bigint              	                    
title               	string              	                    
air_date            	bigint              	                    
                    	                    	                    
# Partitioning      	                    	                    
Not partitioned     	                    	                    
spark-sql> -- Run the simple Spark SQL query on top of YDB table
spark-sql> SELECT * FROM ydb.`python-examples`.`basic`.episodes LIMIT 5;
1	1	1	Yesterday's Jam	13182
1	1	2	Calamity Jen	13182
1	1	3	Fifty-Fifty	13189
1	1	4	The Red Door	13196
1	1	5	The Haunting of Bill Crouse	13203
```

Below there are some read operations using Scala:

```
spark.table("ydb.test0_fhrw").select("created_date", "complaint_type", "city").show(10, false);

spark.table("ydb.test0_fhrw").write().parquet("test0_fhrw");
```