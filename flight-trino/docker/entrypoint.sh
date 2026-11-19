#!/usr/bin/env bash
set -euo pipefail
heap="${TRINO_HEAP:-1G}"
if [[ ! "$heap" =~ ^[1-9][0-9]*[MG]$ ]]; then
    echo "TRINO_HEAP must be a positive value in M or G" >&2
    exit 2
fi
sed -i "s/^-Xmx.*/-Xmx${heap}/" /etc/trino/jvm.config
mkdir -p /etc/trino/catalog
cat > /etc/trino/config.properties <<'CONFIG'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory=512MB
query.max-memory-per-node=256MB
memory.heap-headroom-per-node=256MB
CONFIG
if [[ -n "${FLIGHT_CATALOG_FILE:-}" ]]; then
    cp -- "$FLIGHT_CATALOG_FILE" /etc/trino/catalog/flight.properties
else
    printf '%s
' \
        'connector.name=arrow_flight' \
        "flight.host=${FLIGHT_HOST:-flight-server-1}" \
        "flight.port=${FLIGHT_PORT:-32010}" \
        "flight.bearer-token=${FLIGHT_BEARER_TOKEN:-local-development}" \
        "flight.tls.enabled=${FLIGHT_TLS_ENABLED:-false}" \
        > /etc/trino/catalog/flight.properties
fi
exec /usr/lib/trino/bin/run-trino "$@"
