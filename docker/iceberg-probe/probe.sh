#!/usr/bin/env bash
set -euo pipefail

probe_dir=/iceberg-probe
fixture_file=/tmp/iceberg-probe.tsv
run_root="hdfs://hdfs-namenode:8020/iceberg-probe/$(cat /proc/sys/kernel/random/uuid)"

"${SPARK_HOME}/bin/spark-submit" \
  --master 'local[2]' \
  --jars /opt/iceberg-spark-runtime.jar \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.probe=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.probe.type=hadoop \
  --conf spark.sql.catalog.probe.cache-enabled=false \
  --conf "spark.sql.catalog.probe.warehouse=${run_root}/warehouse" \
  "${probe_dir}/fixture.py" "${run_root}" "${fixture_file}"

if [[ "${1:-}" == "--spark-only" ]]; then
  echo "PASS Spark verified the Iceberg fixture on HDFS: ${run_root}"
  cat "${fixture_file}"
  exit 0
fi

hadoop_cp="$("${HADOOP_HOME}/bin/hadoop" classpath --glob)"
export CLASSPATH="${APP_JAR}:${HADOOP_CONF_DIR}:${hadoop_cp}:${SPARK_HOME}/jars/*${CLASSPATH:+:${CLASSPATH}}"
exec java -DduckDbWarmConnections=0 "-DdataDir=${run_root}" \
  -cp "${probe_dir}/classes:${CLASSPATH}" IcebergHdfsProbe "${fixture_file}"
