# Cluster Service

Документ описывает, как `ClusterService` координирует несколько Flight-серверов: регистрация, heartbeat, инвентаризация файлов, отслеживание нагрузки и жизненный цикл statement cache.

## Обзор

`ClusterService` запускается на каждом Flight-сервере. Использует Hazelcast distributed maps для обмена состоянием между нодами кластера. Каждая нода регистрируется, публикует инвентарь своих файлов и поддерживает heartbeat. Сервис используется во время планирования запроса для обнаружения живых нод, поиска файлов и балансировки нагрузки.

## Компоненты

`ClusterService` оборачивает `HazelcastAdapter`. Hazelcast предоставляет четыре distributed maps:

| Map              | Ключ                   | Значение             | Назначение                               |
|------------------|------------------------|----------------------|------------------------------------------|
| `serverRegistry` | `flight-server-N:32010`| `Long` (байты нагр.) | Регистрация ноды + текущая нагрузка      |
| `serverHeartbeats`| `flight-server-N:32010`| `Long` (timestamp)   | Heartbeat метки времени для проверки alive|
| `serverFiles`    | `flight-server-N:32010`| `Map<String, Long>`  | Инвентарь файлов ноды (путь -> байты)    |
| `statementCache` | handle (UUID строка)   | `HandleState`        | Состояние запроса, TTL 10 минут          |

## Регистрация и дерегистрация сервера

### Запуск

При старте Flight-сервера конструктор `ClusterService`:

```java
hazelcast.serverRegistry().put(serverUri, 0L);
hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
```

Нода записывает свой URI (например `flight-server-1:32010`) в обе maps с начальной нагрузкой 0.

### Остановка

`ClusterService.close()` удаляет ноду из обеих maps:

```java
hazelcast.serverRegistry().remove(serverUri);
hazelcast.serverHeartbeats().remove(serverUri);
```

Это предотвращает появление мёртвых записей при штатном завершении.

## Heartbeat механизм

После регистрации scheduled executor отправляет heartbeats каждые 15 секунд:

```java
heartbeatExecutor.scheduleAtFixedRate(() -> {
    hazelcast.serverRegistry().putIfAbsent(serverUri, 0L);
    hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
}, 15, 15, TimeUnit.SECONDS);
```

Executor — один daemon-тред с именем `flight-heartbeat`. Каждый heartbeat пишет текущий timestamp в `serverHeartbeats`.

### Проверка живости

Перед планированием запроса `FlightSqlProducer` вызывает `ClusterService.filterLiveServers()`. Метод читает все heartbeats, отфильтровывает ноды, чей heartbeat старше 45 секунд, и удаляет мёртвые из обеих maps:

```java
long deadline = now - 45_000; // 45 секунд без heartbeat = мёртв
for (String uri : serverUris) {
    Long lastHb = heartbeats.get(uri);
    if (lastHb == null) {
        // Новая нода, heartbeat ещё не было — считаем живой
        live.add(uri);
    } else if (lastHb >= deadline) {
        live.add(uri);
    } else {
        // Старая запись — удаляем из кластера
        hazelcast.serverRegistry().remove(uri);
        hazelcast.serverHeartbeats().remove(uri);
    }
}
```

Нода без heartbeat (только зарегистрировалась, heartbeat ещё не отправился) считается живой. Это закрывает короткое окно между регистрацией и первым heartbeat.

Константы времени:

| Параметр                 | Значение | Назначение                              |
|--------------------------|----------|-----------------------------------------|
| `HEARTBEAT_INTERVAL_SEC` | 15s      | Частота записи heartbeat каждой нодой   |
| `HEARTBEAT_TIMEOUT_SEC`  | 45s      | Через сколько без heartbeat нода мертва |

### Нагрузка серверов

Нагрузка хранится как одно `Long` значение на сервер в `serverRegistry`. Изначально 0, увеличивается при создании handle запроса (поле `bytes` из `HandleState`) и уменьшается при истечении или явном удалении handle.

`ClusterService.addLoad(uri, delta)` добавляет signed delta к нагрузке сервера:

```java
hazelcast.serverRegistry().compute(uri, (k, v) -> {
    if (v == null) return delta;
    long updated = v + delta;
    return updated <= 0 ? 0L : updated;
});
```

Два метода для чтения нагрузки:
- `allServerLoads()` — нагрузка всех зарегистрированных серверов
- `getLoads(serverUris)` — нагрузка конкретного набора серверов

## Инвентаризация файлов

После запуска каждый Flight-сервер сканирует свою директорию данных и вызывает `registerLocalFiles()`:

```java
hazelcast.serverFiles().put(this.serverUri, new LinkedHashMap<>(files));
```

Файлы сохраняются как относительные пути с размером в байтах. Создаётся распределённый инвентарь по всем нодам.

### Поиск расположения файлов

При планировании запроса `FlightSqlProducer` вызывает `fileLocations()`, чтобы получить полную картину того, где лежит каждый Parquet-файл:

```java
public Map<String, FileAssignment> fileLocations() {
    Set<String> inventoryServers = hazelcast.serverFiles().keySet();
    Map<String, Map<String, Long>> inventories = hazelcast.serverFiles().getAll(inventoryServers);
    Map<String, FileAssignment> result = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, Long>> server : inventories.entrySet()) {
        String serverUri = server.getKey();
        for (Map.Entry<String, Long> file : server.getValue().entrySet()) {
            result.compute(file.getKey(), (path, current) -> {
                Set<String> owners = new LinkedHashSet<>();
                long size = file.getValue();
                if (current != null) {
                    if (current.size() != size) {
                        throw new IllegalStateException(
                            "Conflicting sizes for replicated file " + path);
                    }
                    owners.addAll(current.hosts());
                }
                owners.add(serverUri);
                return new FileAssignment(size, owners);
            });
        }
    }
    return result;
}
```

Результат — map `относительный_путь -> FileAssignment(размер, [хост1, хост2, ...])`. Если один файл есть на нескольких нодах — это реплика, все хосты сохраняются. `FileAssignment` бросает исключение, если у реплик разный размер.

`hasFileInventory(serverUri)` проверяет, опубликовала ли конкретная нода свой инвентарь.

## Жизненный цикл Signed Ticket и Load Lease

### Создание Ticket

Во время `GetFlightInfo` сервер кодирует endpoint state в ticket, подписанный
HMAC-SHA256. Общий signing secret один раз распространяется через
`statementCache`. Для shard с одним владельцем per-query distributed entry не
создается.

Только для реплицированных shard сервер сохраняет load lease с TTL 10 минут.
Так нагрузка очистится, даже если клиент не вызовет `DoGet`.

### Разрешение Ticket

Во время `DoGet` целевая нода локально проверяет и декодирует ticket. Старые
UUID handles сначала ищутся в local TTL cache, затем в Hazelcast.

### Удаление Load Lease

После выполнения replicated shard удаление lease и уменьшение load идемпотентны.
Shard с одним владельцем пропускает обе операции.

### Истечение Load Lease

Если lease не удален из-за disconnect или crash, Hazelcast вытесняет его через
10 минут. Существующий `EntryExpiredListener` вычитает bytes из нагрузки
сервера-владельца. Если сервер уже удален из registry, listener не создает его
заново.

## Взаимодействие с выполнением запроса

Во время `GetFlightInfo`:

1. `FlightSqlProducer` вызывает `filterLiveServers()` для получения активных нод.
2. `ClusterService.fileLocations()` предоставляет distributed file inventory.
3. `QueryPlanner` переиспользует или обновляет table file-plan cache.
4. `pickServer()` выбирает лучшую ноду для каждого файла: предпочитает ноды, на которых есть блоки файла, и выбирает наименее загруженную среди подходящих.
5. Каждый endpoint получает signed self-contained ticket.
6. Для replicated assignment дополнительно создается load lease на 10 минут.

Во время `DoGet`:

1. Ticket локально проверяется и декодируется.
2. Восстанавливаются назначенные файлы и SQL.
3. `ExecutionService` читает и стримит результат.
4. Для replicated shard по завершению удаляется load lease.

Если `DoGet` не приходит для replicated assignment, lease истекает через
10 минут и нагрузка корректируется expiry listener.

## Сводка констант времени

| Константа                         | Значение | Область     | Назначение                                  |
|-----------------------------------|----------|-------------|---------------------------------------------|
| `HEARTBEAT_INTERVAL_SEC`          | 15s      | Cluster     | Интервал между записями heartbeat           |
| `HEARTBEAT_TIMEOUT_SEC`           | 45s      | Cluster     | Нода считается мёртвой после такого молчания |
| load lease TTL                    | 10min    | Statement   | Максимальное время жизни load reservation     |
| `flightListenerReadyTimeoutMs`    | 60s      | DuckDB      | Максимальное ожидание готовности клиента Flight|
| `hazelcastClusterJoinTimeoutSec`  | 60s      | Hazelcast   | Максимальное ожидание формирования кластера  |
