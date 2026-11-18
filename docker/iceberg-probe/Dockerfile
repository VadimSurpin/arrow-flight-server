ARG FLIGHT_RUNTIME_IMAGE=arrowflight-iceberg-runtime:local
FROM ${FLIGHT_RUNTIME_IMAGE} AS spark-fixture
RUN curl -fL --retry 3 --retry-all-errors --connect-timeout 15 --max-time 180 \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    -o /opt/iceberg-spark-runtime.jar
COPY fixture.py probe.sh /iceberg-probe/

FROM eclipse-temurin:21-jdk-jammy AS probe-compiler
COPY --from=spark-fixture /opt/arrowflight/arrow-flight-server.jar /tmp/server.jar
COPY IcebergHdfsProbe.java /tmp/IcebergHdfsProbe.java
RUN javac --release 21 -cp /tmp/server.jar -d /classes /tmp/IcebergHdfsProbe.java

FROM spark-fixture
COPY --from=probe-compiler /classes /iceberg-probe/classes
