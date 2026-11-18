#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_files=(-f "${repo_root}/docker/iceberg-probe/compose.yml")
runtime_image=arrowflight-iceberg-runtime:local
build_context="${repo_root}"
dockerfile="${repo_root}/docker/Dockerfile"

if [[ "${1:-}" == "--spark-only" && $# == 1 ]]; then
  compose_files+=(-f "${repo_root}/docker/iceberg-probe/compose.spark.yml")
  runtime_image=arrowflight-iceberg-spark:local
  build_context="${repo_root}/docker"
  dockerfile="${repo_root}/docker/iceberg-probe/Dockerfile.spark"
elif (( $# != 0 )); then
  echo "Usage: $0 [--spark-only]" >&2
  exit 2
fi

if [[ -z "${FLIGHT_RUNTIME_IMAGE:-}" ]]; then
  export FLIGHT_RUNTIME_IMAGE="${runtime_image}"
  docker build -f "${dockerfile}" \
    --build-arg "UBUNTU_MIRROR=${UBUNTU_MIRROR:-https://archive.ubuntu.com/ubuntu}" \
    -t "${FLIGHT_RUNTIME_IMAGE}" "${build_context}"
fi

exec docker compose "${compose_files[@]}" up --build --exit-code-from probe
