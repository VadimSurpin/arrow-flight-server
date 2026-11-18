"""Creates the Parquet control and the Iceberg snapshots for the JDBC probe."""

import csv
import sys

from pyspark.sql import SparkSession


TABLE = "probe.sales.orders"


def capture_snapshot(spark, label, expected):
    """Returns the metadata file and ID for the current, verified snapshot."""
    rows = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").collect()
    actual = [row.id for row in rows]
    if actual != expected:
        raise RuntimeError(f"Spark {label}: expected {expected}, got {actual}")

    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'main'"
    ).first().snapshot_id
    metadata = spark.sql(
        f"SELECT file FROM {TABLE}.metadata_log_entries "
        f"WHERE latest_snapshot_id = {snapshot} ORDER BY timestamp DESC LIMIT 1"
    ).first().file
    return label, metadata, snapshot


def main(run_root, fixture_file):
    """Writes independent control data and records three pinned snapshots."""
    spark = SparkSession.builder.appName("Iceberg HDFS read probe").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        seed = spark.createDataFrame([(1,), (2,)], "id BIGINT").coalesce(1)
        control = f"{run_root}/control"
        seed.write.parquet(control)
        (control_file,) = spark.read.parquet(control).inputFiles()

        spark.sql("CREATE NAMESPACE probe.sales")
        spark.sql(
            f"CREATE TABLE {TABLE} (id BIGINT) USING iceberg "
            "TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', "
            "'write.delete.mode'='merge-on-read')"
        )
        seed.writeTo(TABLE).append()
        initial = capture_snapshot(spark, "initial snapshot", [1, 2])

        spark.sql(f"INSERT INTO {TABLE} VALUES (3)")
        appended = capture_snapshot(spark, "after append", [1, 2, 3])

        spark.sql(f"DELETE FROM {TABLE} WHERE id = 2")
        if spark.sql(f"SELECT * FROM {TABLE}.files WHERE content = 1").isEmpty():
            raise RuntimeError("Spark did not produce a position delete file")
        deleted = capture_snapshot(spark, "after position delete", [1, 3])

        with open(fixture_file, "w", encoding="utf-8", newline="") as fixture:
            writer = csv.writer(fixture, delimiter="\t", lineterminator="\n")
            writer.writerow([control_file])
            writer.writerows([initial, appended, deleted])
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
