import os

from pyspark.sql import SparkSession

FLIGHT_SOURCE_PROVIDER = "pro.surpin.data.arrowflight.client.spark.FlightSource"


def quote_identifier(value):
    return "`" + value.replace("`", "``") + "`"


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def hdfs_location(root, *parts):
    suffix = "/".join(str(part).strip("/") for part in parts)
    return f"{root.rstrip('/')}/{suffix}"


def list_hdfs_tables(spark, root, schema):
    schema_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_location(root, schema))
    filesystem = schema_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(schema_path):
        return []

    tables = []
    for status in filesystem.listStatus(schema_path):
        if not status.isDirectory():
            continue
        files = filesystem.listFiles(status.getPath(), True)
        has_parquet = False
        while files.hasNext():
            file = files.next()
            if file.isFile() and file.getPath().getName().lower().endswith(".parquet"):
                has_parquet = True
                break
        if has_parquet:
            tables.append(status.getPath().getName())
    return sorted(tables)


def create_database(spark, schema):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(schema)}")


def register_flight_tables(spark, publish_schema, source_schema, tables, host, port):
    if not tables:
        raise RuntimeError(f"No Flight Parquet tables found for schema '{source_schema}'")

    create_database(spark, publish_schema)
    for table in tables:
        spark_table = f"{quote_identifier(publish_schema)}.{quote_identifier(table)}"
        source_table = f"{source_schema}.{table}"
        spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
        spark.sql(
            f"""
            CREATE TABLE {spark_table}
            USING {FLIGHT_SOURCE_PROVIDER}
            OPTIONS (
              host {sql_string(host)},
              port {sql_string(port)},
              user 'user',
              password 'password',
              `tls.enabled` 'false',
              table {sql_string(source_table)}
            )
            """
        )
        if not spark.table(spark_table).schema.fields:
            raise RuntimeError(
                f"Flight returned an empty schema for {source_table}; "
                f"verify the Parquet shard mounted on {host}"
            )
        print(f"Registered Spark Flight table {publish_schema}.{table} via flight://{host}:{port}/{source_table}")


def register_direct_parquet_tables(spark, publish_schema, source_schema, tables, hdfs_root):
    if not tables:
        raise RuntimeError(f"No HDFS Parquet tables found for schema '{source_schema}' under {hdfs_root}")

    create_database(spark, publish_schema)
    for table in tables:
        spark_table = f"{quote_identifier(publish_schema)}.{quote_identifier(table)}"
        location = hdfs_location(hdfs_root, source_schema, table)
        spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
        spark.sql(
            f"""
            CREATE TABLE {spark_table}
            USING parquet
            LOCATION {sql_string(location)}
            """
        )
        print(f"Registered Spark direct Parquet table {publish_schema}.{table} at {location}")


dataset = os.environ.get("BENCHMARK_DATASET", "tpch").lower()
source_schema = os.environ.get("BENCHMARK_SOURCE_SCHEMA", os.environ.get("BENCHMARK_SCHEMA", dataset)).lower()
publish_schema = os.environ.get("SPARK_PUBLISH_SCHEMA") or os.environ.get("BENCHMARK_SCHEMA", source_schema)
publish_mode = os.environ.get("BENCHMARK_PUBLISH_MODE", "flight").lower()
flight_publish_schema = os.environ.get("FLIGHT_PUBLISH_SCHEMA", f"{source_schema}_flight")
direct_publish_schema = os.environ.get("DIRECT_PUBLISH_SCHEMA", f"{source_schema}_direct")
hdfs_root = os.environ.get(
    "HDFS_DATA_DIR",
    os.environ.get("DIRECT_PARQUET_DIR", "hdfs://hdfs-namenode:8020/bench"),
)
flight_host = os.environ.get("FLIGHT_SOURCE_HOST", "flight-server-1")
flight_port = os.environ.get("FLIGHT_SOURCE_PORT", "32010")

spark = (
    SparkSession.builder.appName("BenchBaseSparkTablePublisher")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

tables = list_hdfs_tables(spark, hdfs_root, source_schema)

if publish_mode == "flight":
    register_flight_tables(spark, publish_schema, source_schema, tables, flight_host, flight_port)
elif publish_mode == "direct":
    register_direct_parquet_tables(spark, publish_schema, source_schema, tables, hdfs_root)
elif publish_mode == "both":
    register_flight_tables(spark, flight_publish_schema, source_schema, tables, flight_host, flight_port)
    register_direct_parquet_tables(spark, direct_publish_schema, source_schema, tables, hdfs_root)
else:
    raise RuntimeError("BENCHMARK_PUBLISH_MODE must be flight, direct, or both")

spark.stop()
