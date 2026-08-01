# Arrow Flight Server — opencode agent instructions

See [README.md](README.md) for project overview, build/test commands, tech stack, architecture, CI/CD.

## Code conventions

- **Javadoc**: Required on **every** class and every non-`@Override` method. Format: single sentence summary, then `@param`, `@return`, `@throws`. No HTML tags, no implementation details. Short.
- **Checkstyle** + **SpotBugs**: enforced in CI. Exclusions only via `spotbugs-exclude.xml` with inline rationale comment.
- **Tests**: JUnit 5 with tags (`unit`, `integration`, `spark`, `perf`, `smoke`). Mockito, no PowerMock.
- **No preview features**, Java 21 only.
- Follow existing patterns when adding code (check neighboring files for style).
- New code paths must be covered by unit tests. Bug fixes must include a regression test.

Run `./mvnw compile checkstyle:check spotbugs:check && ./mvnw test` after every code change.
