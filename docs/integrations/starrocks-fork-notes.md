# StarRocks fork notes (branch-4.0, `feat/flight-connector`)

Fork: `/home/vadim/arrowflight-server/starrocks` (shallow clone of `StarRocks/starrocks` branch `branch-4.0`,
local branch `feat/flight-connector`). Build wrapper: `scripts/build-starrocks-fork.sh` (passes args to `build.sh`).

## 1. Reference files (all exist)

- FE: `fe/fe-core/src/main/java/com/starrocks/connector/{elasticsearch,jdbc}/`
- BE: `be/src/connector/{connector.cpp,es_connector.cpp,jdbc_connector.cpp}` (headers `*.h` beside them)
- `ConnectorType.java` lines: `ES("es", ElasticsearchConnector.class, EsConfig.class)`, `JDBC("jdbc", JDBCConnector.class, null)`.

## 2. FE connector SPI (`fe/fe-core/src/main/java/com/starrocks/connector/`)

- `public interface Connector extends MemoryTrackable`
  - abstract: `ConnectorMetadata getMetadata();`
  - defaults: `void shutdown()`, `void bindConfig(ConnectorConfig config)`, `boolean supportMemoryTrack()` (false),
    `Map<String, Long> estimateCount()`.
- `public interface ConnectorMetadata` has NO abstract methods. Every method is `default` (listDbNames(ConnectContext),
  listTableNames(ConnectContext, String dbName), getTable(ConnectContext, dbName, tblName), getTableType(),
  tableExists, dbExists, getDb(ConnectContext, name), getTableStatistics(OptimizerContext, Table, Map<ColumnRefOperator, Column>,
  List<PartitionKey>, ScalarOperator predicate, long limit, TableVersionRange), getRemoteFiles, shutdown(), ...).
  Override only what the flight catalog needs. `getTableType()` is a hint of `Table.TableType`.
- `ConnectorFactory`: static `CatalogConnector createConnector(ConnectorContext, boolean isReplay)` (wraps `LazyConnector`,
  `InformationSchemaConnector`, `TableMetaConnector`) and static `Connector createRealConnector(ConnectorContext)`.
  `createRealConnector` does `ConnectorType.from(context.getType())`, then reflectively calls the connector class's
  constructor `getDeclaredConstructor(ConnectorContext.class)`. If the enum entry has a non-null config class it does
  `configClass.newInstance().loadConfig(context.getProperties())` then `connector.bindConfig(config)`.
- `ConnectorContext(String catalogName, String type, Map<String,String> properties)`.
- Registration is solely the `ConnectorType` enum: add an entry `FLIGHT("flight", FlightConnector.class, <ConfigClass|null>)`
  to BOTH the enum constants and `ConnectorType.SUPPORT_TYPE_SET` (an `EnumSet.of(...)`, must list the new entry).
  `ConnectorType.isSupport(type)` is used by `CatalogMgr` and `CatalogAnalyzer` (so no further analyzer change is needed).
- `JDBCConnector`: ctor `JDBCConnector(ConnectorContext)` reads `context.getProperties()`, validates required props, builds
  `JDBCMetadata(properties, catalogName)` eagerly (falls back to lazy in `getMetadata()`), `shutdown()` delegates to metadata.
  `ElasticsearchConnector`: ctor stores `catalogName`; `bindConfig(ConnectorConfig)` casts to `EsConfig` and creates the REST
  client; `getMetadata()` lazily builds `ElasticsearchMetadata`.
- FE plan nodes: `fe/fe-core/src/main/java/com/starrocks/planner/EsScanNode.java`, `JDBCScanNode.java` (`toThrift(TPlanNode)`).

## 3. Thrift (`gensrc/thrift/PlanNodes.thrift`)

- Enum `TPlanNodeType`: `ES_SCAN_NODE`, `ES_HTTP_SCAN_NODE`, `JDBC_SCAN_NODE`, `LAKE_SCAN_NODE`, ... (no generic connector type; the
  generic `TConnectorScanNode` struct exists but is used for BE-internal routing).
- `TPlanNode` fields: `30: optional TEsScanNode es_scan_node`, `60: optional TJDBCScanNode jdbc_scan_node`,
  `61: optional TConnectorScanNode connector_scan_node` (highest observed scan-node field ids; check the file for the next free id).
- `struct TEsScanNode { 1: required TTupleId tuple_id; 2: optional map<string,string> properties; 3: optional map<string,string> docvalue_context; ...}`.
- `struct TJDBCScanNode { 1: optional TTupleId tuple_id; 2: optional string table_name; 3: optional list<string> columns; 4: optional list<string> filters; 5: optional i64 limit }`.
- `struct TConnectorScanNode { 1: optional string connector_name }` (BE sets it; FE does not).
- `struct TScanRange { 4: TInternalScanRange internal_scan_range; 5: binary kudu_scan_token (deprecated); 6: TBrokerScanRange broker_scan_range;
  7: TEsScanRange es_scan_range; 20: THdfsScanRange hdfs_scan_range; 30: TBinlogScanRange binlog_scan_range }`.
- `struct TEsScanRange { 1: required list<TNetworkAddress> es_hosts; 2: required string index; 3: optional string type; 4: required i32 shard_id }`.
- JDBC has no scan-range payload (BE `JDBCDataSource(provider, TScanRange)` ignores it).
- FE `EsScanNode.toThrift`: `ES_HTTP_SCAN_NODE` when `table.getTransport()` is HTTP, otherwise `ES_SCAN_NODE`.

## 4. BE connector registration (`be/src/connector/`)

- `connector.h`: `class Connector` with `virtual DataSourceProviderPtr create_data_source_provider(ConnectorScanNode*, const TPlanNode&) const`
  and `virtual ConnectorType connector_type() const = 0` (enum `ConnectorType` {HIVE=0, ES=1, JDBC=2, MYSQL=3, FILE=4, LAKE=5, BINLOG=6, ICEBERG=7});
  static name strings `Connector::HIVE/ES/JDBC/MYSQL/FILE/LAKE/BINLOG/ICEBERG` defined in `connector.cpp`.
- `connector.cpp`: file-scope `ConnectorManagerInit _init` ctor calls
  `ConnectorManager::default_instance()->put(Connector::ES, std::make_unique<ESConnector>());` etc. A new connector needs: a name string,
  a `ConnectorType` enum value, a `put(...)` line, and its .cpp in `be/src/connector/CMakeLists.txt` (es_connector.cpp / jdbc_connector.cpp listed there).
- `DataSourceProvider` (pure virtual: `create_data_source(const TScanRange&)`, `tuple_descriptor(RuntimeState*)`); `DataSource` (pure virtual:
  `name()`, `raw_rows_read()`, `num_rows_read()`, `num_bytes_read()`, `cpu_time_spent()`, `set_offset`, `reset_status`,
  `num_rows_read_in_epoch`, `cpu_time_spent_in_epoch`; `open/get_next/close` virtual with defaults).
- Provider creation: `be/src/exec/exec_node.cpp` `case TPlanNodeType::JDBC_SCAN_NODE` copies the TPlanNode, sets
  `connector_scan_node.connector_name = connector::Connector::JDBC`, and does `new ConnectorScanNode(pool, new_node, descs)`.
  `ConnectorScanNode` ctor (`be/src/exec/connector_scan_node.cpp`) does
  `ConnectorManager::default_instance()->get(tnode.connector_scan_node.connector_name)->create_data_source_provider(this, tnode)`.
  So a new FE plan-node type needs a `case` in `exec_node.cpp` plus the Thrift enum/struct.

## 5. Risk gate 3: Arrow Flight in BE thirdparty -- PASS

- `thirdparty/build-thirdparty.sh` `build_arrow()` passes `-DARROW_FLIGHT=ON` and `-DARROW_FLIGHT_SQL=ON` (lines 907-908). Arrow version in thirdparty
  is 19.0.1 (`thirdparty/vars.sh`: `arrow-apache-arrow-19.0.1`).
- `be/CMakeLists.txt` lines 959-960 link `arrow_flight` and `arrow_flight_sql`. BE already has `be/src/service/service_be/arrow_flight_sql_service.h`.
- No thirdparty change is required for Task 7. (A Flight CLIENT needs only these same libs plus gRPC, already in thirdparty.)
- FE Arrow: `fe/pom.xml` `<arrow.version>18.0.0</arrow.version>`; FE `fe-core` depends on `arrow-vector`, `arrow-memory-netty`, `arrow-compression`
  (managed in root pom around line 1357). `flight-core` is NOT a direct dependency (the pom comments note it arrives transitively and is left unmanaged).

## 6. Risk gate 4: can this machine build the BE?

Measured:
- RAM 11 GB total (~8 GB available), swap 3 GB, 4 cores (Xeon E3-1230 V2, Ivy Bridge: SSE4.2, NO AVX2), disk 26 GB free of 250 GB.
- `https://github.com`, `archive.apache.org`, `repo.maven.apache.org` reachable. `releases.starrocks.io` returns HTTP 403 (HEAD and GET).
- Prebuilt thirdparty tarball IS reachable: `https://cdn-thirdparty.starrocks.com/starrocks-thirdparty-main-20250731.tar` (HTTP 200, 3.9 GB,
  see `docker/dockerfiles/dev-env/dev-env.Dockerfile`). Measured download speed from this host was about 240 KB/s (about 4.5 hours).
- `build.sh` (even for `--fe`) runs `thirdparty/build-thirdparty.sh` unless `thirdparty/installed/llvm/lib/libLLVMInstCombine.a` exists. That is a
  full from-source build of LLVM, Arrow, gRPC, etc.: many hours and tens of GB on this machine.
- Findings: BE build is NOT practical here without a multi-hour prebuilt-tarball download (it is compiled for the StarRocks dev-env distro, ABI
  match with the conda toolchain unverified) plus a BE compile of thousands of C++ files at 4 cores / 11 GB (LTO-less link of the BE needs
  several GB). Additionally the BE assumes AVX2 in places; this CPU lacks it, so a BE UT run may crash with illegal instruction.
  Tasks 5-7 (BE C++) should plan on either a different build host, or on code-only authoring verified by syntax/`-fsyntax-only` against fork headers.
  Not stopped, per controller ruling.

## 7. Toolchain fixes needed for the FE build (all under git-ignored `tools/.local/env`)

1. `env.sh` of the fork needs `gcc` on PATH; the conda env only ships `x86_64-conda-linux-gnu-gcc`. Fix: symlinks `gcc`, `g++`, `ar`, `ranlib`,
   `nm`, `ld`, `objcopy`, `objdump`, `strip`, `as`, `readelf` -> `x86_64-conda-linux-gnu-*` in `tools/.local/env/bin`.
2. `build.sh` triggers a full thirdparty build unless the LLVM marker exists. For FE-only work create the marker and tool links in the fork
   (untracked, ignored): `thirdparty/installed/llvm/lib/libLLVMInstCombine.a` (empty file), `thirdparty/installed/bin/{thrift,protoc}` -> env binaries.
3. `protoc` 25.3 installed via `micromamba install -p tools/.local/env -c conda-forge libprotobuf=4.25.3`. No conda package ships the thrift
   COMPILER, so thrift 0.24.0 was built from `thirdparty/src/thrift-0.24.0.tar.gz` (cmake `-DBUILD_COMPILER=ON -DBUILD_LIBRARIES=OFF`, needs conda
   `bison` and `flex`) and copied to `tools/.local/env/bin/thrift`.
4. IMPORTANT: this CPU lacks x86-64-v3, but the conda sysroot stamps every linked executable with a v3 ISA note, so glibc refuses to run them
   ("CPU ISA level is lower than required"). Fix per binary: `objcopy --remove-section .note.gnu.property <binary>`. Anything built later with the
   conda gcc (including a BE and its tests) needs this, or link with `-Wl,-z,...` equivalents.

## 8. Test invocations for this fork

- FE unit test (one class): `cd /home/vadim/arrowflight-server/starrocks && ./run-fe-ut.sh --test com.starrocks.connector.jdbc.JDBCMetadataTest`
  (script wraps `mvn test -DfailIfNoTests=false -D test=<name> -T <parallel>` in `fe/`; set `PATH`/`JAVA_HOME` as in the wrapper script; `-j<N>` for parallelism).
  Direct form: `cd fe && mvn test -pl fe-core -DfailIfNoTests=false -Dtest=<Class>`.
- BE unit test (gtest): `./run-be-ut.sh --test <BinaryOrCaseGlob>` (appends `*`) or `./run-be-ut.sh --gtest_filter 'SuiteName*:Other*'`.
  Requires full thirdparty (see section 6). Test sources are listed in `be/test/CMakeLists.txt`.
- Build wrapper: `./scripts/build-starrocks-fork.sh --fe` (and `--be`).

## 9. Baseline FE build result

`./scripts/build-starrocks-fork.sh --fe --clean` succeeded after the fixes in section 7 ("Successfully build StarRocks - Frontend", about 40 min
wall clock including first-time Maven dependency download at 10-200 kB/s); `starrocks/output/fe` exists. The fork working tree stays clean
(`git status` empty; `thirdparty/installed`, `output`, `fe/**/target` are ignored).

## C++ tests on hosts without AVX2

`flight-starrocks/cpp` links Arrow C++ 25.0.0 from conda-forge (`libarrow-flight-sql` installed into
`tools/.local/env`); the StarRocks BE thirdparty uses Arrow 19.0.1, so the vendored reader must stay within
the API common to both. Binaries from the conda toolchain fail with "CPU ISA level is lower than required" on
CPUs without AVX2. Configure with `-DFLIGHT_STARROCKS_STRIP_ISA_NOTE=ON` and run with
`LD_LIBRARY_PATH=tools/.local/env/lib`.
