#!/usr/bin/env bash
# Starts one clustered Flight server node over a local Parquet directory.
set -euo pipefail
exec java \
  --add-modules java.se,jdk.httpserver \
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens java.management/sun.management=ALL-UNNAMED \
  --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
  -Dio.netty.tryReflectionSetAccessible=true \
  ${JAVA_OPTS:-} \
  ${FLIGHT_DUCKDB_THREADS:+-DduckDbThreads=$FLIGHT_DUCKDB_THREADS} \
  -cp "/opt/arrowflight/arrow-flight-server.jar:/opt/arrowflight/hadoop/*" \
  pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
  --data-dir "${FLIGHT_DATA_DIR:-/data/parquet}" \
  --port "${FLIGHT_PORT:-32010}" \
  --hosts "${FLIGHT_HOSTS}" \
  --localhost "${FLIGHT_LOCALHOST:-$(hostname -f)}" \
  --storage-host "${FLIGHT_LOCAL_STORAGE_HOST:-$(hostname -f)}" \
  --hazelcast-port "${HAZELCAST_PORT:-5701}" \
  --metrics-port "${FLIGHT_METRICS_PORT:-9404}" \
  "$@"
