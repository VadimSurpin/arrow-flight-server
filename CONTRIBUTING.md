# Contributing to Arrow Flight Server

Thank you for your interest in contributing! This guide covers everything you need to get started.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Code Conventions](#code-conventions)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Reporting Bugs](#reporting-bugs)
- [Proposing Features](#proposing-features)

---

## Getting Started

1. Fork the repository and clone your fork.
2. Create a feature branch from `main`: `git checkout -b feat/my-feature`.
3. Make your changes, add tests, and verify CI passes locally.
4. Open a pull request against `main`.

---

## Development Setup

**Requirements**: Java 21, Maven (or use the included `./mvnw` wrapper — no Maven install needed).

```bash
# Build without tests
./mvnw package -DskipTests

# Start a single-node server against test data
java -jar target/hadoop-arrow-flight-*.jar \
    --data-dir src/test/resources/test_db \
    --localhost 127.0.0.1 \
    --port 32010 \
    --hosts 127.0.0.1
```

See [docs/user_guides/build-test-and-scripts.md](docs/user_guides/build-test-and-scripts.md) for the full guide.

---

## Running Tests

```bash
# Unit tests only (fast, no external services)
./mvnw test

# Integration + Spark + smoke tests (requires local FS; takes a few minutes)
./mvnw test -P integration

# Performance benchmarks
./mvnw test -P perf
```

Run linting before submitting:

```bash
./mvnw compile checkstyle:check spotbugs:check
```

---

## Code Conventions

- **Javadoc** is required on every class and every non-`@Override` method: single-sentence summary, then `@param`/`@return`/`@throws`. No HTML tags. Keep it short.
- **Checkstyle** and **SpotBugs** are enforced in CI and will fail the build. Suppressions go in `spotbugs-exclude.xml` with a rationale comment.
- **Tests** use JUnit 5 with tags: `unit`, `integration`, `spark`, `perf`, `smoke`. No PowerMock.
- New code paths must have unit test coverage. Bug fixes must include a regression test.
- Java 21 only — no preview features.
- Match the style of the file you are editing. When in doubt, check a neighboring file.

---

## Submitting a Pull Request

1. **One concern per PR.** Mix of unrelated changes slows review.
2. Ensure `./mvnw compile checkstyle:check spotbugs:check && ./mvnw test` passes locally.
3. Write a clear PR description: what changed, why, and how to test it.
4. Link any related issue with `Fixes #123` or `Closes #123`.
5. Keep commits focused. Squash fixup commits before marking ready for review.

CI checks run automatically. All jobs must be green before a PR is merged.

---

## Reporting Bugs

Use the **Bug Report** issue template. Include:
- Java version and OS
- Cluster size and data layout (number of nodes, Parquet file count and size)
- The query that failed
- Full stack trace or error message
- Steps to reproduce

---

## Proposing Features

Open a **Feature Request** issue before writing code for non-trivial changes. Describe the problem you want to solve and why the current approach falls short. This avoids duplicate work and lets us discuss the design early.

For significant architectural changes, consider writing an ADR in `docs/adr/` (see existing examples for the format).
