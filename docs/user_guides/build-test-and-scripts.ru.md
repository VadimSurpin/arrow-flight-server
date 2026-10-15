# Сборка, тесты и run.sh

Этот guide описывает, как поднять проект у себя или на сервере, как запускать тесты и как пользоваться `run.sh`.

## Требования

Нужны:

- Linux/WSL или серверная Linux-машина
- JDK 21
- Maven
- Git
- Hadoop/Spark окружение, если запускаются HDFS/Spark сценарии

Проверь версии:

```bash
java -version
mvn -version
git --version
```

## Получить ветку

Из корня репозитория:

```bash
git fetch origin
git switch feature/goto-duckdb
git pull --ff-only
```

Если локальной ветки еще  нет:

```bash
git switch --track origin/feature/goto-duckdb
```

Проверить текущую ветку:

```bash
git status --short --branch
```

## Сборка

Быстрая компиляция:

```bash
mvn -q compile
```

Полная сборка jar:

```bash
mvn -q clean package
```

Итоговый shaded jar:

```text
flight-server/target/arrow-flight-server-1.0.0-shaded.jar
```

## Unit tests

По умолчанию integration tests исключены через Maven property `excludedGroups=integration`.

Запуск обычных тестов:

```bash
mvn -q clean test
```

Проверить exit code:

```bash
echo $?
```

`0` означает, что Maven test phase прошел успешно.

Важно: в логах могут быть строки `ERROR` от negative tests parser-а. Например тесты специально проверяют, что `UPDATE ...` или multi-table `FROM a, b` падают с исключением. Если Maven завершился с кодом `0`, это не ошибка.

## Integration tests

Запустить integration tests вместе с остальными:

```bash
mvn -q -DexcludedGroups="" test
```

Запустить конкретный integration test:

```bash
mvn -q -DexcludedGroups="" -Dtest=ParquetManagerIntegrationTest test
mvn -q -DexcludedGroups="" -Dtest=AggregationIntegrationTest test
```

Для Windows эти тесты могут падать из-за Hadoop `winutils` / `HADOOP_HOME`. На сервере или в WSL это обычно не проблема.

## Performance tests

Запустить perf benchmark:

```bash
mvn -q -DexcludedGroups="" -Dgroups=perf -Dtest=ArrowFlightPerfTest test
```

Настроить размер данных и число прогонов:

```bash
mvn -q -DexcludedGroups="" -Dgroups=perf -Dtest=ArrowFlightPerfTest \
  -Dperf.rows=500000 \
  -Dperf.runs=5 \
  test
```

## Runtime config

Основной config:

```text
flight-server/src/main/resources/arrowflight.properties
```

Ключевые параметры:

- `batchSize` - размер Arrow batch для DuckDB export и Flight streaming.
- `ioParallelism` - явное число worker threads. Если пустой, считается по формуле.
- `ioParallelismMinThreads` - нижняя граница thread pool.
- `ioParallelismMaxCores` - максимум CPU cores для расчета; `0` значит без ограничения.
- `ioParallelismMultiplier` - коэффициент умножения cores, по умолчанию `8`.
- `duckDbWarmConnections` - сколько DuckDB connections прогревать при старте.
- `duckDbGroups` - сколько групп использовать для старых grouped helper paths.
- `duckDbThreads` - `SET threads` для DuckDB connection.
- `grpcMaxInboundMessageSize` - max inbound gRPC message size.
- `flightBackpressureThresholdBytes` - максимум сериализованных outbound bytes в очереди до включения Flight backpressure.
- `flightListenerReadyTimeoutMs` - timeout ожидания готовности Flight listener.

Формула `ioParallelism`, если он не задан явно:

```text
max(ioParallelismMinThreads, min(availableProcessors, ioParallelismMaxCores) * ioParallelismMultiplier)
```

JVM system properties переопределяют значения из файла:

```bash
mvn test -Darrowflight.io.parallelism=64
mvn test -Darrowflight.duckdb.threads=2
```

## HDFS runtime

HDFS Parquet-файлы открывает DuckDB через настроенный HDFS extension. В Linux runtime должны быть доступны `libhdfs`, Hadoop configuration и Java classpath Hadoop. Docker image и entrypoint проекта задают для этого `LD_LIBRARY_PATH`, `HADOOP_CONF_DIR` и `CLASSPATH`.

## run.sh

`run.sh` - helper для серверной проверки ветки.

Он умеет:

- выбрать ветку
- сделать `git fetch`
- переключиться на ветку
- fast-forward локальную ветку до `origin/<branch>`
- выполнить `mvn compile`
- запустить выбранные тесты

По умолчанию `run.sh` не destructive. Он не выкидывает локальные изменения и использует `git merge --ff-only`.

Если нужно ровно сбросить ветку к `origin/<branch>`, добавь `--force-reset`. Если рабочая копия dirty, скрипт покажет `git status --short` и попросит явно ввести подтверждение перед destructive action.

Примеры:

```bash
chmod +x run.sh

./run.sh -b feature/goto-duckdb -t all
./run.sh -b feature/goto-duckdb -t ArrowFlightPerfTest
./run.sh -b feature/goto-duckdb -t perf -r 500000 -n 5
./run.sh -b main -t all --force-reset
```

Режимы:

- `-t all` - обычный `mvn test`.
- `-t perf` - `ArrowFlightPerfTest` с group `perf`.
- `-t <ClassName>` - конкретный test class.
- `-r ROWS` - `perf.rows`.
- `-n RUNS` - `perf.runs`.
- `--force-reset` - выполнить `git reset --hard origin/<branch>` после явного подтверждения, если worktree dirty.
