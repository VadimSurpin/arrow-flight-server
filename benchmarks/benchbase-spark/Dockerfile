ARG BENCHBASE_IMAGE=benchbase.azurecr.io/benchbase:latest
ARG HIVE_JDBC_VERSION=2.3.9

FROM eclipse-temurin:21-jdk-jammy AS shim-builder

ARG HIVE_JDBC_VERSION

USER root
RUN mkdir -p /benchbase/lib
ADD https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/${HIVE_JDBC_VERSION}/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar /benchbase/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar
RUN chmod 0644 /benchbase/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar
COPY shim /benchbase/shim
RUN mkdir -p /benchbase/classes \
    && javac -cp "/benchbase/lib/*" -d /benchbase/classes $(find /benchbase/shim -name '*.java') \
    && java -cp "/benchbase/classes:/benchbase/lib/*" pro.surpin.benchbase.HiveExecuteDriverSelfTest \
    && rm /benchbase/classes/net/surpin/benchbase/HiveExecuteDriverSelfTest.class

FROM ${BENCHBASE_IMAGE}

ARG HIVE_JDBC_VERSION

USER root
RUN mkdir -p /benchbase/lib
ADD https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/${HIVE_JDBC_VERSION}/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar /benchbase/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar
RUN chmod 0644 /benchbase/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar
COPY --from=shim-builder /benchbase/classes /benchbase/classes
COPY benchbase-entrypoint.sh /benchbase-entrypoint.sh
RUN chmod +x /benchbase-entrypoint.sh

ENTRYPOINT ["/benchbase-entrypoint.sh"]
