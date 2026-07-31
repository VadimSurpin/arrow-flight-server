# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Apache-2.0 LICENSE
- `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`
- Issue and PR templates
- `dependabot.yml` for automated dependency updates
- Release workflow triggered on `v*` tags
- `CODEOWNERS` for automatic review routing
- Package renamed from `net.surpin` to `pro.surpin`

### Changed
- GitHub Actions pinned to full commit SHAs (supply-chain hardening)
- `groupId` updated from `com.example` to `pro.surpin.arrowflight`
- SonarCloud project key and organization updated to `VadimSurpin`
- README badges and clone URLs updated to `VadimSurpin/arrow-flight-server`
- `commons-cli` updated from 1.5.0 to 1.9.0

### Removed
- `.gitlab-ci.yml` (GitHub Actions is the canonical CI)

---

## [1.0.0] — 2026-07-28

Initial public release. Forked from [nsu-fit/ArrowFlight](https://github.com/nsu-fit/ArrowFlight).

### Added
- Arrow Flight SQL server running as a sidecar on each Hadoop DataNode or Ozone storage node
- Parquet footer fast-path for `COUNT(*)`, `MIN`, `MAX` with zero column I/O
- DuckDB in-process aggregation engine receiving Arrow C streams from Parquet scanner
- Hazelcast-based cluster formation and node registry (zero manual configuration)
- Size-aware file scheduler: assigns Parquet files to nodes by total bytes, TTL-cached
- Prometheus metrics endpoint with per-query execution-path tracking
- Spark DataSource V2 connector with column projection, predicate pushdown, and aggregate pushdown
- Columnar read path (`FlightArrowColumnVector`) — no row conversion in Spark
- `FlightSessionCatalog` for Spark catalog integration
- `TypeConversionHelper` with explicit Arrow↔Spark type mapping (fixes INT_8 ClassCastException)
- Dagger 2 dependency injection replacing manual wiring
- Multi-stage Docker image (`maven:3.9.9` build + `eclipse-temurin:21-jre-jammy` runtime)
- `docker-compose.yml` orchestrating 8 services: 3 Flight nodes, Spark cluster, Grafana, Prometheus
- TPC-H and TPC-DS benchmark suite (`benchmarks/benchbase-spark/`)
- GitHub Actions CI: build, lint (Checkstyle + SpotBugs), integration tests, JaCoCo coverage
- Architecture Decision Records for distribution strategy, benchmark suite, DI choice, BenchBase bridge
- Architecture documentation: query execution flow, Parquet storage model, cluster service

### Changed
- All Russian-language comments translated to English
- Package restructured into `adapters/`, `services/`, `model/`, `metrics/` sub-packages
- Substrait filter pushdown replaced by DuckDB JDBC execution
- `HadoopFlightSqlService` God class decomposed into `FlightSqlProducer`, `ExecutionService`, `QueryPlanner`, `ClusterService`
- Java version bumped from 17 to 21
- Checkstyle and SpotBugs now fail the build on violations
- JaCoCo coverage reporting with SonarCloud integration

[Unreleased]: https://github.com/VadimSurpin/arrow-flight-server/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/VadimSurpin/arrow-flight-server/releases/tag/v1.0.0
