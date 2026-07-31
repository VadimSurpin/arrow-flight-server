#!/usr/bin/env bash
set -euo pipefail

mode="${1:-server}"
shift || true

DEFAULT_SERVER_JAVA_OPTS=(
  --add-modules java.se,jdk.httpserver
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens java.management/sun.management=ALL-UNNAMED
  --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
  -Dio.netty.tryReflectionSetAccessible=true
)

DEFAULT_SPARK_JAVA_OPTIONS="${DEFAULT_SPARK_JAVA_OPTIONS:-\
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=java.base/sun.security.action=ALL-UNNAMED \
-Dio.netty.tryReflectionSetAccessible=true}"

DEFAULT_HIVE_METASTORE_SHARED_PREFIXES="${SPARK_HIVE_METASTORE_SHARED_PREFIXES:-\
pro.surpin.data.arrowflight,flight,org.apache.arrow,io.grpc,io.netty,com.google.protobuf}"

export SPARK_DAEMON_JAVA_OPTS="${SPARK_DAEMON_JAVA_OPTS:-${DEFAULT_SPARK_JAVA_OPTIONS}}"
export HDFS_BLOCK_SIZE_BYTES="${HDFS_BLOCK_SIZE_BYTES:-1073741824}"

DEFAULT_HADOOP_JAVA_OPTS="${DEFAULT_HADOOP_JAVA_OPTS:-\
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-exports=java.base/sun.net.dns=ALL-UNNAMED \
--add-exports=java.base/sun.net.util=ALL-UNNAMED}"
export HADOOP_OPTS="${HADOOP_OPTS:-${DEFAULT_HADOOP_JAVA_OPTS}}"
export HDFS_NAMENODE_OPTS="${HDFS_NAMENODE_OPTS:-${DEFAULT_HADOOP_JAVA_OPTS}}"
export HDFS_DATANODE_OPTS="${HDFS_DATANODE_OPTS:-${DEFAULT_HADOOP_JAVA_OPTS}}"

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local timeout_seconds="${3:-60}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for ${host}:${port}" >&2
  return 1
}

expose_app_jar_to_spark_driver() {
  export SPARK_CLASSPATH="${APP_JAR}${SPARK_CLASSPATH:+:${SPARK_CLASSPATH}}"
  export SPARK_DIST_CLASSPATH="${APP_JAR}${SPARK_DIST_CLASSPATH:+:${SPARK_DIST_CLASSPATH}}"
}

hadoop_classpath() {
  "${HADOOP_HOME}/bin/hadoop" classpath --glob
}

flight_server_command() {
  local hadoop_cp
  hadoop_cp="$(hadoop_classpath)"
  local runtime_classpath="${APP_JAR}:${HADOOP_CONF_DIR}:${hadoop_cp}:${SPARK_HOME}/jars/*"
  # The DuckDB HDFS extension can load libhdfs at runtime. libhdfs reads
  # CLASSPATH from the process environment; java's -cp alone is not sufficient.
  export CLASSPATH="${runtime_classpath}${CLASSPATH:+:${CLASSPATH}}"
  local java_opts=("${DEFAULT_SERVER_JAVA_OPTS[@]}")
  if [[ -n "${JAVA_OPTS:-}" ]]; then
    local extra_java_opts=()
    read -r -a extra_java_opts <<< "${JAVA_OPTS}"
    java_opts+=("${extra_java_opts[@]}")
  fi
  if [[ -n "${TIMING_LOG_LEVEL:-}" ]]; then
    java_opts+=("-Dorg.slf4j.simpleLogger.log.Timing=${TIMING_LOG_LEVEL}")
  fi
  if [[ -n "${FLIGHT_BATCH_SIZE:-}" ]]; then
    java_opts+=("-DbatchSize=${FLIGHT_BATCH_SIZE}")
  fi
  if [[ -n "${FLIGHT_DUCKDB_THREADS:-}" ]]; then
    java_opts+=("-DduckDbThreads=${FLIGHT_DUCKDB_THREADS}")
  fi

  local command=(java "${java_opts[@]}" \
    -cp "${runtime_classpath}" \
    pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
    --data-dir "${FLIGHT_DATA_DIR:-/data/parquet}" \
    --port "${FLIGHT_PORT:-32010}" \
    --hosts "${FLIGHT_HOSTS:-flight-server-1,flight-server-2,flight-server-3,flight-server-4,flight-server-5,flight-server-6,flight-server-7,flight-server-8,flight-server-9,flight-server-10}" \
    --localhost "${FLIGHT_LOCALHOST:-$(hostname -f)}" \
    --storage-host "${FLIGHT_LOCAL_STORAGE_HOST:-$(hostname -f)}" \
    --hazelcast-port "${HAZELCAST_PORT:-5701}" \
    --metrics-port "${FLIGHT_METRICS_PORT:-9404}" \
    "$@")
  if [[ "${FLIGHT_SERVER_EXEC:-false}" == "true" ]]; then
    exec "${command[@]}"
  fi
  "${command[@]}"
}

wait_for_hdfs_path() {
  local path="$1"
  local watched_pid="${2:-}"
  while ! "${HADOOP_HOME}/bin/hdfs" dfs -test -e "${path}" >/dev/null 2>&1; do
    if [[ -n "${watched_pid}" ]] && ! kill -0 "${watched_pid}" 2>/dev/null; then
      echo "HDFS DataNode exited before ${path} became ready" >&2
      return 1
    fi
    sleep 2
  done
}

stop_children() {
  local pid
  for pid in "$@"; do
    kill "${pid}" 2>/dev/null || true
  done
  for pid in "$@"; do
    wait "${pid}" 2>/dev/null || true
  done
}

spark_submit_common() {
  local app_file="$1"
  shift
  wait_for_tcp "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 120
  expose_app_jar_to_spark_driver
  exec "${SPARK_HOME}/bin/spark-submit" \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --jars "${APP_JAR}" \
    --driver-class-path "${APP_JAR}" \
    --driver-java-options "${SPARK_DRIVER_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}" \
    --conf "spark.executor.extraJavaOptions=${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}" \
    --conf "spark.driver.extraClassPath=${APP_JAR}" \
    --conf "spark.executor.extraClassPath=${APP_JAR}" \
    --conf "spark.driver.userClassPathFirst=true" \
    --conf "spark.executor.userClassPathFirst=true" \
    --conf "spark.driver.bindAddress=0.0.0.0" \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST:-$(hostname -f)}" \
    --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-8}" \
    --conf "spark.sql.ansi.enabled=${SPARK_SQL_ANSI_ENABLED:-true}" \
    --conf "spark.sql.decimalOperations.allowPrecisionLoss=${SPARK_DECIMAL_ALLOW_PRECISION_LOSS:-true}" \
    --conf "spark.sql.catalogImplementation=hive" \
    --conf "spark.sql.hive.metastore.sharedPrefixes=${DEFAULT_HIVE_METASTORE_SHARED_PREFIXES}" \
    --conf "spark.sql.warehouse.dir=${SPARK_WAREHOUSE_DIR:-/spark-warehouse}" \
    --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=${SPARK_METASTORE_DB:-/spark-warehouse/metastore_db};create=true" \
    "${app_file}" \
    "$@"
}

spark_common_conf=(
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}"
  --jars "${APP_JAR}"
  --driver-class-path "${APP_JAR}"
  --driver-java-options "${SPARK_DRIVER_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}"
  --conf "spark.executor.extraJavaOptions=${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS:-${DEFAULT_SPARK_JAVA_OPTIONS}}"
  --conf "spark.driver.extraClassPath=${APP_JAR}"
  --conf "spark.executor.extraClassPath=${APP_JAR}"
  --conf "spark.driver.userClassPathFirst=true"
  --conf "spark.executor.userClassPathFirst=true"
  --conf "spark.driver.bindAddress=0.0.0.0"
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-$(hostname -f)}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-8}"
  --conf "spark.sql.ansi.enabled=${SPARK_SQL_ANSI_ENABLED:-true}"
  --conf "spark.sql.decimalOperations.allowPrecisionLoss=${SPARK_DECIMAL_ALLOW_PRECISION_LOSS:-true}"
  --conf "spark.sql.catalogImplementation=hive"
  --conf "spark.sql.catalog.spark_catalog=pro.surpin.data.arrowflight.client.spark.FlightSessionCatalog"
  --conf "spark.sql.hive.metastore.sharedPrefixes=${DEFAULT_HIVE_METASTORE_SHARED_PREFIXES}"
  --conf "spark.sql.warehouse.dir=${SPARK_WAREHOUSE_DIR:-/spark-warehouse}"
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=${SPARK_METASTORE_DB:-/spark-warehouse/metastore_db};create=true"
)

case "${mode}" in
  server)
    FLIGHT_SERVER_EXEC=true flight_server_command "$@"
    ;;
  hdfs-namenode)
    mkdir -p /var/lib/hadoop-hdfs/name /var/lib/hadoop-hdfs/tmp
    if [[ ! -f /var/lib/hadoop-hdfs/name/current/VERSION ]]; then
      "${HADOOP_HOME}/bin/hdfs" namenode -format -force -nonInteractive
    fi
    exec "${HADOOP_HOME}/bin/hdfs" namenode
    ;;
  benchmark-node)
    mkdir -p /var/lib/hadoop-hdfs/data /var/lib/hadoop-hdfs/tmp /var/lib/hadoop-hdfs/socket
    wait_for_tcp "${HDFS_NAMENODE_HOST:-hdfs-namenode}" "${HDFS_NAMENODE_PORT:-8020}" 180

    "${HADOOP_HOME}/bin/hdfs" datanode &
    datanode_pid="$!"
    child_pids=("${datanode_pid}")
    trap 'stop_children "${child_pids[@]}"' TERM INT EXIT

    wait_for_hdfs_path "${HDFS_READY_PATH:-/bench/_READY}" "${datanode_pid}"

    flight_server_command &
    flight_pid="$!"
    child_pids+=("${flight_pid}")

    wait_for_tcp "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 180
    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --cores "${SPARK_WORKER_CORES:-2}" \
      --memory "${SPARK_WORKER_MEMORY:-2g}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
      "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
      "$@" &
    worker_pid="$!"
    child_pids+=("${worker_pid}")

    set +e
    wait -n "${child_pids[@]}"
    child_status="$?"
    set -e
    stop_children "${child_pids[@]}"
    trap - TERM INT EXIT
    exit "${child_status}"
    ;;
  spark-master)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST:-spark-master}" \
      --port "${SPARK_MASTER_PORT:-7077}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}" \
      "$@"
    ;;
  spark-worker)
    wait_for_tcp "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 120
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --cores "${SPARK_WORKER_CORES:-2}" \
      --memory "${SPARK_WORKER_MEMORY:-2g}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
      "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
      "$@"
    ;;
  publish-benchmark-data)
    spark_submit_common "${APP_HOME}/spark/publish_benchbase_tables.py" "$@"
    ;;
  spark-thrift-server)
    wait_for_tcp "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 120
    expose_app_jar_to_spark_driver
    exec "${SPARK_HOME}/bin/spark-submit" \
      "${spark_common_conf[@]}" \
      --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
      "${SPARK_HOME}/jars/spark-hive-thriftserver_2.12-3.5.9.jar" \
      --hiveconf "hive.server2.thrift.bind.host=${SPARK_THRIFT_BIND_HOST:-0.0.0.0}" \
      --hiveconf "hive.server2.thrift.port=${SPARK_THRIFT_PORT:-10000}" \
      "$@"
    ;;
  *)
    exec "${mode}" "$@"
    ;;
esac
