# syntax=docker/dockerfile:1

ARG UBUNTU_MIRROR=http://mirror.yandex.ru/ubuntu

# Stage 1: Build duckdb-hdfs extension
FROM eclipse-temurin:21-jdk-jammy AS duckdb-builder
ARG UBUNTU_MIRROR
WORKDIR /build
COPY docker/scripts/configure_apt_mirror.sh docker/scripts/build_duckdb_hdfs.sh ./
RUN chmod +x configure_apt_mirror.sh build_duckdb_hdfs.sh \
    && UBUNTU_MIRROR="${UBUNTU_MIRROR}" ./build_duckdb_hdfs.sh

# Stage 2: Download Spark
FROM eclipse-temurin:21-jre-jammy AS spark-downloader
ARG UBUNTU_MIRROR
WORKDIR /opt
COPY docker/scripts/configure_apt_mirror.sh docker/scripts/download_spark.sh ./
RUN chmod +x configure_apt_mirror.sh download_spark.sh \
    && UBUNTU_MIRROR="${UBUNTU_MIRROR}" ./download_spark.sh

# Stage 3: Download Hadoop
FROM eclipse-temurin:21-jre-jammy AS hadoop-downloader
ARG UBUNTU_MIRROR
WORKDIR /opt
COPY docker/scripts/configure_apt_mirror.sh docker/scripts/download_hadoop.sh ./
RUN chmod +x configure_apt_mirror.sh download_hadoop.sh \
    && UBUNTU_MIRROR="${UBUNTU_MIRROR}" ./download_hadoop.sh

# Stage 4: Build Java application
FROM maven:3.9.9-eclipse-temurin-21 AS java-builder
WORKDIR /src
COPY pom.xml checkstyle.xml spotbugs-exclude.xml ./
RUN mvn -q dependency:go-offline dependency:resolve-plugins
COPY src/ ./src/
RUN mvn -q -DskipTests package

# Stage 5: Final runtime image
FROM eclipse-temurin:21-jre-jammy
ARG UBUNTU_MIRROR

ENV APP_HOME=/opt/arrowflight
ENV APP_JAR=${APP_HOME}/arrow-flight-server.jar
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# Copy artifacts from previous stages
COPY --from=duckdb-builder /opt/duckdb-extensions /opt/duckdb-extensions
COPY --from=spark-downloader /opt/spark /opt/spark
COPY --from=hadoop-downloader /opt/hadoop /opt/hadoop
# Glob picks up the versioned JAR regardless of version string (e.g. arrow-flight-server-1.0.0.jar).
# The shade plugin renames the original to original-*.jar, so original-* is never matched here.
COPY --from=java-builder /src/target/arrow-flight-server-*.jar ${APP_JAR}

# FIX 1: Установить runtime-зависимости для libhdfs3 (они были в рабочем Dockerfile, но пропали здесь!)
# Без них libhdfs3.so не загрузится, и DuckDB не поймет протокол hdfs://
COPY docker/scripts/configure_apt_mirror.sh /usr/local/bin/configure-apt-mirror
RUN chmod +x /usr/local/bin/configure-apt-mirror \
    && UBUNTU_MIRROR="${UBUNTU_MIRROR}" /usr/local/bin/configure-apt-mirror \
    && apt-get update \
    && apt-get install -y --no-install-recommends bash curl procps tini python3 \
        libuuid1 libprotobuf23 libxml2 libkrb5-3 libgsasl7 \
    && rm -rf /var/lib/apt/lists/* \
    && rm /usr/local/bin/configure-apt-mirror

# FIX 2: Вернуть критически важные переменные окружения для DuckDB HDFS
ENV DUCKDB_HDFS_HOME=/opt/duckdb-extensions
ENV DUCKDB_HDFS_EXTENSION=/opt/duckdb-extensions/hadoopfs.duckdb_extension
ENV DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true
ENV LD_LIBRARY_PATH=/opt/duckdb-extensions:/opt/hadoop/lib/native:/opt/java/openjdk/lib/server
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}

WORKDIR ${APP_HOME}
COPY docker/bin/ ./bin/
COPY docker/hadoop/ ${HADOOP_CONF_DIR}/
COPY docker/spark/ ./spark/
RUN chmod +x ./bin/*.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/arrowflight/bin/entrypoint.sh"]
CMD ["server"]
