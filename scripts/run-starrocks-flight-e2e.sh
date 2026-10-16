#!/usr/bin/env bash
set -euo pipefail

# Requires Docker and enough memory for the StarRocks all-in-one test image.
# The test runs the Flight SQL fixture in the Maven JVM and the FE/BE container
# in host-network mode so the backend can redeem Flight endpoints on localhost.
./mvnw -pl flight-starrocks-e2e -am verify \
  -Dtest=StarRocksFlightCatalogE2ETest \
  -Dgroups=real-services \
  -DexcludedGroups= \
  -Dsurefire.failIfNoSpecifiedTests=false
