# Поток выполнения SQL-запроса

Этот документ описывает путь SQL-запроса через Arrow Flight сервер: от `GetFlightInfo` до чтения Parquet-файлов, выбора движка выполнения и отправки Arrow batch-ей клиенту.

## Основные компоненты

`HadoopArrowFlightServer` запускает сервер и настраивает Hadoop FileSystem, Hazelcast и Arrow Flight.

`FlightSqlProducer` реализует слой Flight SQL. Он принимает запросы, строит `FlightInfo`, создает `Ticket`, распределяет файлы по Flight-нодам и восстанавливает состояние запроса во время `DoGet`.

`ExecutionService`, `MetadataService` и `ParquetAdapter` отвечают за Parquet-часть. `ParquetAdapter` занимается поиском файлов и локальностью блоков. `MetadataService` строит схему результата и реализует Java fast path по footer metadata. `ExecutionService` координирует выполнение SQL через DuckDB.

`ParquetQueryParser` разбирает SQL и извлекает схему, таблицу, список колонок, фильтр, агрегаты и `GROUP BY`.

`AppConfig` / `ConfigAdapter` читает настройки из `arrowflight.properties`. JVM system properties и часть environment variables могут переопределять значения из файла.

## Общий поток

Запрос проходит две фазы.

1. Планирование: клиент вызывает `GetFlightInfo`; сервер определяет схему результата, распределяет файлы между Flight-нодами и возвращает endpoints с tickets.
2. Чтение: клиент вызывает `DoGet` по tickets; соответствующие Flight-ноды читают назначенные файлы и стримят результат в Arrow-формате.

Данные не читаются во время `GetFlightInfo`. Реальное чтение начинается только во время `DoGet`.

## Получение SQL-запроса

SQL приходит в `FlightSqlProducer.getFlightInfoStatement`.

На этом этапе сервер:

1. Строит или переиспользует закэшированную Arrow-схему результата.
2. Строит endpoints из закэшированного распределенного file inventory.
3. Вычисляет оценки размера Parquet-файлов и числа строк из footer.
4. Возвращает эти оценки в `FlightInfo`.

## Hazelcast cache и ticket

Ticket каждого endpoint содержит SQL, относительные пути назначенных файлов,
целевой сервер, оценку байтов и флаг load accounting. Payload подписывается
HMAC-SHA256 общим кластерным секретом, созданным через Hazelcast. Поэтому любая
Flight-нода проверяет ticket без distributed lookup.

Когда у каждого shard один владелец, планирование endpoint и `DoGet` не хранят
query state в Hazelcast. Для реплицированных shard Hazelcast хранит только
10-минутный load lease, чтобы брошенный ticket не оставлял нагрузку навсегда.
Старые UUID handles продолжают работать через local/distributed fallback cache.

## Построение схемы результата

Перед возвратом `FlightInfo` сервер должен знать схему результата. Для этого `FlightSqlProducer` вызывает `MetadataService.getQuerySchema`.

Для обычных `SELECT` схема строится из Parquet schema таблицы и выбранных колонок. Для агрегатов схема строится из aggregate expressions и `GROUP BY`.

## Распределение файлов

Распределение выполняется в `QueryPlanner.plan`.

Planner разбирает SQL и фильтрует distributed file inventory по используемым
таблицам. Распределение table-to-files кэшируется на 30 секунд и немедленно
обновляется, если текущий состав кластера делает cached plan невалидным.

Дальше сервер читает список зарегистрированных Flight-нод из `serverRegistry`. Для каждого файла вызывается `pickServer`, который выбирает ноду для чтения.

Если Flight-нода находится на host, где есть блоки файла, такая нода предпочитается. Среди подходящих нод выбирается нода с меньшей накопленной нагрузкой по размеру назначенных файлов.

Для каждой группы файлов создается отдельный endpoint.

## Чтение по ticket

Когда клиент вызывает `DoGet`, сервер попадает в `FlightSqlProducer.getStreamStatement`.

Сервер проверяет подпись ticket, локально декодирует SQL и список назначенных
файлов, затем вызывает `ExecutionService.readParquet`.

Именно здесь начинается выполнение запроса.

## Выбор query engine

В проекте есть два пути чтения и выполнения Parquet-запросов. Выбор делается в `ExecutionService.readParquet` после разбора SQL.

Правила маршрутизации:

1. Metadata-only aggregates -> Java footer path.
2. Full scan, projection, `WHERE`, `GROUP BY`, `SUM`, fallback aggregates и `JOIN` -> DuckDB.

Java используется там, где результат можно получить из Parquet footer без чтения data pages; все остальные запросы выполняются DuckDB.

## Java footer path

Java footer path используется для простых агрегатов без `WHERE` и без `GROUP BY`.

Поддерживаемые fast path выражения:

- `COUNT(*)`
- `COUNT(col)`, если есть полная статистика null-count
- `MIN(col)`, если есть min statistics
- `MAX(col)`, если есть max statistics

Для `COUNT(*)` Java суммирует row counts из metadata row group-ов. Для `COUNT(col)` Java вычитает null counts. Для `MIN` и `MAX` Java объединяет статистики по row group-ам.

Этот путь не читает data pages и не запускает DuckDB для самого запроса. Если нужной статистики нет, запрос безопасно fallback-ится в DuckDB.

## DuckDB path

DuckDB используется для всех запросов, которые нельзя выполнить из Parquet footer metadata:

- full scan и projection
- `WHERE`
- фильтрованные projection
- `GROUP BY`
- `SUM`
- агрегаты, которые не удалось выполнить из footer statistics
- `JOIN`

`ExecutionService` строит SQL поверх DuckDB table function `read_parquet([...])`. Alias-ы join-таблиц создаются как временные DuckDB views над соответствующими Parquet inputs.

DuckDB возвращает результат через `DuckDBResultSet.arrowExportStream`. Сервер копирует строки из DuckDB Arrow stream в Flight batches и отправляет клиенту.

Для локальных файлов DuckDB extension не нужен. Для HDFS URI требуется настроенный DuckDB HDFS extension.

## Runtime tuning

Основной файл настроек: `src/main/resources/arrowflight.properties`.

Главные streaming-настройки - `batchSize` и `flightBackpressureThresholdBytes`.
Они задают размер DuckDB Arrow export batch и объём сериализованных данных, который Flight может отправлять конвейером до ожидания клиента.

Параллелизм I/O считается так:

`max(ioParallelismMinThreads, min(availableProcessors, ioParallelismMaxCores) * ioParallelismMultiplier)`

Если `ioParallelism` задан явно, используется он. `ioParallelismMaxCores=0` означает "не ограничивать число ядер".

Пример override через JVM system property:

```bash
mvn test -Darrowflight.io.parallelism=64
```

## Отправка результата клиенту

Обычный `SELECT` обычно отправляет несколько Arrow batch-ей. Агрегации часто отправляют один итоговый batch, но это зависит от запроса и количества групп.

Отправка выполняется через `ServerStreamListener`. `VectorSchemaRoot` содержит текущий Arrow batch, а `putNext` отправляет его клиенту.

После завершения чтения сервер завершает stream.

## Текущие ограничения

Работа распределяется на уровне целых файлов. Один очень большой файл не делится между несколькими Flight-нодами.

Подписанный endpoint ticket самодостаточен. Только load lease для
реплицированных shard зависит от Hazelcast, причем его истечение влияет на
accounting, а не на декодирование ticket.

Фактическое выполнение запроса начинается только во время `DoGet`. `GetFlightInfo` отвечает за планирование, схему результата, endpoints и tickets.
