# ADR 0005: Прослойка BenchBase–Spark через HiveExecuteDriver

Статус: Принято
Дата: 2026-07-14

## Запись решения

- Время: 2026-07-14T17:00:00+07:00
- Пользователь: s.samokhin
- Вопрос: BenchBase использует JDBC (диалект Postgres) для выполнения бенчмарков.
  Spark SQL не предоставляет стандартного JDBC-интерфейса. Как подружить BenchBase
  со Spark без переписывания обвязки бенчмарков?
- Решение: Вставить тонкий JDBC-прослойку (`HiveExecuteDriver`), который
  транслирует JDBC-вызовы BenchBase в Hive JDBC-вызовы к Spark Thrift Server
  (HiveServer2). Прослойка переписывает Postgres-специфичный SQL-синтаксис
  в Hive-совместимую форму и заглушает операции JDBC, которые Hive не поддерживает.
- Обоснование: Минимум кода (< 250 строк), ноль изменений в BenchBase, полное
  использование Spark Thrift Server, который уже развёрнут. BenchBase видит
  стандартный JDBC-источник; Spark видит стандартного Hive JDBC-клиента.

## Контекст

BenchBase ожидает JDBC-подключение для выполнения TPC-H и TPC-DS нагрузок. Он
генерирует SQL в синтаксисе Postgres: `'1994-01-01'::date`, `?::decimal`,
`concat(?::varchar, ' days')::interval`. Также вызывает `Connection.rollback()`,
`Connection.setAutoCommit(false)`, `Statement.setQueryTimeout()`.

У Spark SQL нет нативного JDBC-драйвера. Он общается по протоколу HiveServer2
через Hive JDBC-драйвер (`jdbc:hive2://`). Hive JDBC-драйвер:

- не поддерживает `?::date` или `::`-синтаксис приведения типов;
- не поддерживает `rollback()` и `setTransactionIsolation()`;
- не понимает `SHOW ALL` (используется BenchBase как прогревочный запрос);
- использует `INTERVAL '1' year`, а не `INTERVAL '1 years'`.

В проекте запущен контейнер Spark Thrift Server (`spark-thrift-server`),
который открывает HiveServer2 на порту 10000. BenchBase работает в отдельном
контейнере (`benchbase-spark`). Им нужно взаимодействовать.

## Архитектура

```
┌──────────────┐    JDBC (диалект Postgres)    ┌──────────────────┐
│  BenchBase   │ ─────────────────────────────▶ │ HiveExecuteDriver│
│  (Java)      │    url=jdbc:hiveexec:hive2://  │  (~230 строк)    │
└──────────────┘    spark-thrift-server:10000/db └────────┬─────────┘
                                                            │
                                               Hive JDBC (jdbc:hive2://)
                                                            │
                                                            ▼
┌───────────────────────────────────────────────────────────────┐
│  Spark Thrift Server (HiveServer2)         порт 10000         │
│  Spark SQL                                                   │
│    ├─ Каталог FlightSource  (tpch_flight) — через Flight SQL  │
│    └─ Каталог Direct        (tpch_direct)  — через Parquet   │
└───────────────────────────────────────────────────────────────┘
```

### Компоненты

**1. `HiveExecuteDriver`** (`shim/net/surpin/benchbase/HiveExecuteDriver.java`)

Реализация `java.sql.Driver`, регистрируемая через `DriverManager`. Делает:

- Перехватывает URL с префиксом `jdbc:hiveexec:`.
- Убирает префикс и делегирует `org.apache.hive.jdbc.HiveDriver`:
  `jdbc:hiveexec:hive2://host:port/db` → `jdbc:hive2://host:port/db`.
- Оборачивает полученные `Connection` и `Statement` в JDK-прокси
  (`InvocationHandler`) для перехвата вызовов.

**2. Прокси подключения** (`ConnectionHandler`)

Заглушает операции, не поддерживаемые HiveServer2:

| Вызов BenchBase            | Поведение прокси          |
|----------------------------|---------------------------|
| `rollback()`               | ничего не делает          |
| `commit()`                 | ничего не делает          |
| `setAutoCommit(false)`     | ничего не делает          |
| `setTransactionIsolation(…)` | ничего не делает        |
| `getAutoCommit()`          | возвращает `true`         |
| `getTransactionIsolation()`| возвращает `TRANSACTION_NONE` |
| `createStatement()`        | оборачивает в `StatementHandler` |
| все остальные              | делегирует Hive-соединению |

**3. Прокси стейтмента** (`StatementHandler`)

Переписывает SQL перед отправкой в Hive (см. `SqlRewrite` ниже). Также
устанавливает `queryTimeout` из переменной окружения
`BENCHBASE_QUERY_TIMEOUT_SECONDS` (по умолчанию 120 с).

**4. `SqlRewrite`** (статические методы в `HiveExecuteDriver.SqlRewrite`)

Преобразует синтаксис Postgres в Hive-совместимый:

| Postgres (BenchBase)                   | Hive (Spark SQL)                   |
|----------------------------------------|------------------------------------|
| `?::date`                              | `CAST(? AS DATE)`                  |
| `'1994-01-01'::date`                   | `DATE '1994-01-01'`                |
| `?::decimal` / `?::decimal(10,2)`      | `CAST(? AS DECIMAL)` / `CAST(? AS DECIMAL(10,2))` |
| `'0.05'::decimal`                      | `0.05`                             |
| `concat(?::varchar, ' days')::interval` | `INTERVAL ? day`                  |
| `INTERVAL '1' year`                    | `INTERVAL '1' year`                |
| `SHOW ALL`                             | BenchBase-compatible engine row    |

### Интеграция

Прослойка компилируется и помещается в classpath внутри контейнера BenchBase
(`benchbase-entrypoint.sh` сканирует `/benchbase/classes`). XML-конфиг
BenchBase указывает:

```xml
<url>jdbc:hiveexec:hive2://spark-thrift-server:10000/tpch_flight</url>
<driver>pro.surpin.benchbase.HiveExecuteDriver</driver>
```

Spark Thrift Server настроен с:

```
spark.sql.catalog.spark_catalog=pro.surpin.data.arrowflight.client.spark.FlightSessionCatalog
spark.sql.hive.metastore.sharedPrefixes=pro.surpin.data.arrowflight,flight,org.apache.arrow,io.grpc,io.netty,com.google.protobuf
```

Параметр `sharedPrefixes` указывает Hive metastore загружать перечисленные
классы из classloader'а приложения (не Hive), избегая `ClassNotFoundException`
для Arrow / Flight / gRPC при планировании запросов.

## Рассмотренные альтернативы

### Вариант A: Кастомный адаптер BenchBase

Написать `BenchmarkDriver`, который общается напрямую с Flight SQL через
Arrow Flight client (gRPC), минуя Spark.

- **Плюсы**: Полный контроль, нет Spark-зависимости, не нужен SqlRewrite.
- **Минусы**: Требует глубокой интеграции с внутренним API BenchBase (не
  предназначен для не-JDBC бэкендов). Нужно реализовать каждый JDBC-метод,
  который вызывает BenchBase, плюс запросы метаданных (`getTables`,
  `getColumns` и т.д.). Рабочие потоки BenchBase ожидают JDBC `ResultSet`.
  Нельзя переиспользовать Spark-планировщик или DataSource V2.

### Вариант B: Spark JDBC-коннектор (DataStax / Simba)

Использовать коммерческий JDBC-драйвер для Spark SQL.

- **Плюсы**: Работает из коробки.
- **Минусы**: DataStax Spark JDBC — проприетарный, требует лицензию. Simba
  ODBC/JDBC — тоже проприетарные. Open-source аналоги (Apache Calcite Avatica,
  Presto JDBC) добавляют лишний сервис и не стыкуются с Spark Thrift Server.

### Вариант C: Прямое чтение Parquet через DuckDB JDBC

Пропустить Spark: BenchBase подключается к DuckDB JDBC, который читает
Parquet-файлы напрямую.

- **Плюсы**: Используется в benchmark-пути без Spark (конфиг `benchbase-entrypoint.sh`
  содержит `url=jdbc:duckdb:...`).
- **Минусы**: Не тестирует Spark + Flight SQL — то, ради чего проект построен.
  Не измеряет end-to-end задержку через Flight SQL сервер.

### Вариант D: HiveExecuteDriver (выбран)

- **Плюсы**: ~230 строк Java. Никаких изменений в BenchBase. Переиспользует
  Spark Thrift Server, который уже развёрнут. SqlRewrite — простые regexp.
  Никаких новых сервисов или зависимостей.
- **Минусы**: SqlRewrite хрупкий — TPC-DS запросы со сложными приведениями
  или интервалами могут потребовать новых паттернов. Прокси Connection/Statement
  заглушает семантику JDBC, на которую иногда полагается BenchBase (например,
  batch-операции пока не нужны).

## Последствия

- **Положительно**: BenchBase запускает любой запрос против любого Spark SQL
  каталога (FlightSource или direct Parquet) простой сменой имени схемы в
  JDBC URL.
- **Положительно**: Режим `compare` исполняет один набор запросов против
  `tpch_flight` (через Flight SQL) и `tpch_direct` (через Parquet напрямую),
  давая сравнение задержек «яблоки к яблокам».
- **Положительно**: `userClassPathFirst=true` в Spark Thrift Server предотвращает
  затенение Arrow/Flight классов Hive JAR-ами при планировании запросов. Без
  этого возникает `ClassNotFoundException` для Arrow-типов.
- **Отрицательно**: `SqlRewrite` нужно синхронизировать, если BenchBase добавит
  новый Postgres-синтаксис или Spark изменит парсер SQL.
- **Отрицательно**: Hive JDBC не поддерживает batch INSERT или
  `Connection.prepareStatement` с generated keys — фаза загрузки данных
  BenchBase не работает через этот шлюз. Загрузка выполняется отдельным
  DuckDB-путем.
- **Отрицательно**: `setQueryTimeout` полагается на переменную окружения
  `BENCHBASE_QUERY_TIMEOUT_SECONDS`. Если BenchBase передаёт таймауты
  по-другому, шлюз может потребовать доработки.
- **Будущее**: Если BenchBase добавит поддержку Arrow Flight, шлюз можно
  будет удалить. Пока это наименее инвазивный мост между JDBC и Spark.
