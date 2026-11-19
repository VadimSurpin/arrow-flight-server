# syntax=docker/dockerfile:1
# Compile only the pinned engine changes; preserve the rest of the official runtime.
FROM trinodb/trino:446 AS stock
FROM maven:3.9.9-eclipse-temurin-21 AS build
RUN apt-get update && apt-get install -y --no-install-recommends git python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY pom.xml checkstyle.xml spotbugs-exclude.xml ./
COPY .mvn/ .mvn/
COPY flight-server/ flight-server/
COPY flight-client-core/ flight-client-core/
COPY flight-starrocks/ flight-starrocks/
COPY flight-spark/pom.xml flight-spark/pom.xml
COPY flight-trino/ flight-trino/
RUN --mount=type=cache,target=/root/.m2 \
    python3 flight-trino/trino-fork/prepare-tests.py \
    && mvn -B -ntp -f flight-trino/reactor/pom.xml -pl :arrow-flight-trino-connector -am \
       -Pintegration,patched-trino test-compile -DskipTests \
    && mvn -B -ntp -f flight-trino/reactor/pom.xml -pl :arrow-flight-trino-connector -am package -DskipTests
COPY --from=stock /usr/lib/trino/lib/trino-main-446.jar /stock/
COPY --from=stock /usr/lib/trino/lib/trino-spi-446.jar /stock/
RUN python3 flight-trino/trino-fork/patch-runtime.py \
    --patch flight-trino/trino-fork/flight-connector.patch \
    --classes flight-trino/target/patched-test-classes --stock /stock --output /patched

FROM stock
USER root
COPY --from=build /opt/java/openjdk/ /opt/java21/
ENV JAVA_HOME=/opt/java21 PATH="/opt/java21/bin:${PATH}"
COPY --chown=trino:trino flight-trino/docker/jvm.config /etc/trino/jvm.config
COPY --from=build --chown=trino:trino /patched/ /usr/lib/trino/lib/
COPY --from=build --chown=trino:trino \
    /src/flight-trino/target/trino-arrow-flight-1.0.0/arrow-flight/ /usr/lib/trino/plugin/arrow-flight/
COPY --chmod=755 flight-trino/docker/entrypoint.sh /usr/local/bin/flight-trino-entrypoint
USER trino
ENTRYPOINT ["/usr/local/bin/flight-trino-entrypoint"]
