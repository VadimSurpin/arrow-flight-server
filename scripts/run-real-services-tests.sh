#!/usr/bin/env bash
set -euo pipefail

required=(
  STARROCKS_JDBC_URL
  FLIGHT_IT_HOST FLIGHT_IT_PORT FLIGHT_IT_CATALOG FLIGHT_IT_SCHEMA FLIGHT_IT_TABLE
  FLIGHT_IT_QUERY
  KRB5_CONFIG FLIGHT_IT_KRB_CLIENT_PRINCIPAL FLIGHT_IT_KRB_CLIENT_KEYTAB
  FLIGHT_IT_KRB_SERVER_PRINCIPAL FLIGHT_IT_KRB_SERVER_KEYTAB FLIGHT_IT_KRB_SERVICE_HOST
  RANGER_ADMIN_URL FLIGHT_IT_RANGER_SERVICE FLIGHT_IT_RANGER_SCHEMA
  FLIGHT_IT_RANGER_ALLOWED_TABLE FLIGHT_IT_RANGER_DENIED_TABLE
  FLIGHT_IT_RANGER_ALLOWED_PRINCIPAL FLIGHT_IT_RANGER_DENIED_PRINCIPAL
  FLIGHT_IT_RANGER_HIDDEN_SCHEMA
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf 'Missing required environment variable: %s\n' "$name" >&2
    exit 2
  fi
done

./mvnw -P real-services -Dgroups=real-services -DexcludedGroups= \
  test --batch-mode --errors --fail-at-end
cmake -S flight-starrocks/cpp -B flight-starrocks/cpp/build
cmake --build flight-starrocks/cpp/build
ctest --test-dir flight-starrocks/cpp/build --output-on-failure
