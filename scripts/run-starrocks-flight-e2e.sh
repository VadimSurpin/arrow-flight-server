#!/usr/bin/env bash
set -euo pipefail

# Requires Docker and enough memory for the StarRocks all-in-one test image.
# The test runs the Flight SQL fixture in the Maven JVM and the FE/BE container
# in host-network mode so the backend can redeem Flight endpoints on localhost.
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
project_version="$(python3 -c 'import pathlib, xml.etree.ElementTree as ET; print(ET.parse(pathlib.Path("pom.xml")).getroot().find("{*}version").text)' )"
./mvnw -pl flight-server -am verify \
  -Dtest=StarRocksFlightCatalogE2ETest \
  -Dgroups=real-services \
  -DexcludedGroups= \
  -Dflight.starrocks.driver.jar="$repo_root/flight-starrocks/target/arrow-flight-starrocks-connector-$project_version-starrocks-driver.jar" \
  -Dflight.server.test.data="$repo_root/flight-server/src/test/resources/test_db" \
  -Dsurefire.failIfNoSpecifiedTests=false
