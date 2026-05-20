# Contributing

## Workflow

1. Branch off `main`: `git checkout -b feat/my-thing`
2. Make changes
3. `mvn verify` — must pass before pushing
4. Open a PR
5. CI runs the same `mvn verify`
6. After review + green CI: squash-merge to `main`

## Code style

- Java 21 with `--enable-preview` if needed (currently not used)
- 4-space indent, no tabs
- Wildcards in imports: avoid except for `package.*` in pom/`<exclusions>`
- Lombok is allowed for boilerplate (`@Getter`, `@Builder`, `@RequiredArgsConstructor`, `@Slf4j`)
- Public methods get Javadoc; private helpers don't unless non-obvious
- Prefer record types for DTOs going forward, but existing Lombok-`@Data` DTOs are fine

## Logging

- Use `@Slf4j` (Lombok-generated SLF4J `log`)
- **Never log credentials, full RIC payloads, or PII**
- For per-RIC events, add `MDC.put("ric", ric)` and `MDC.clear()` in a try/finally — the JSON appender picks it up
- Use `log.debug` for hot-path tick processing; `log.info` for state transitions; `log.error` for things that need a human

## Testing

- Every new service class gets an integration test extending `AbstractIntegrationTest`
- Use unique IDs per test (`"TEST-" + System.nanoTime()`) to avoid cross-test contamination
- Use Awaitility (`org.awaitility.Awaitility.await()...untilAsserted(...)`) for any test that waits on async events

## Adding a Kafka topic

1. Add the topic name to `application.yml` under `marketdata.kafka.topic-*`
2. Add the constant to `MarketDataProperties.Kafka`
3. Add a `@Bean NewTopic` in `KafkaConfig.java` (will be auto-created on startup)
4. Document it in [docs/ARCHITECTURE.md](ARCHITECTURE.md) under "Kafka topics"

## Adding a config property

1. Add it to the relevant nested class in `MarketDataProperties.java` with `@NotNull` / `@Positive` / etc.
2. Set a sensible default in `application.yml`
3. Document it in [docs/CONFIGURATION.md](CONFIGURATION.md)

## Adding a Redis key

1. Add the key/prefix to `MarketDataProperties.Subscription` (or a new sub-class)
2. Document it in [docs/ARCHITECTURE.md](ARCHITECTURE.md) under "Redis keys"
3. Decide on TTL — never store unbounded data in Redis without one

## Adding a database migration

1. Create `src/main/resources/db/migration/V{n}__{description}.sql`
2. Use only `CREATE`, `ALTER`, never `DROP` of data
3. Never modify a Flyway-applied migration after it's merged — write a new one

## Releasing

Versioning is semver: `MAJOR.MINOR.PATCH`.

```bash
# Bump version
mvn versions:set -DnewVersion=1.1.0
# Tag
git commit -am "release: 1.1.0"
git tag v1.1.0
git push --follow-tags
# Build + push image
docker build -t market-data-service:1.1.0 .
docker push <registry>/market-data-service:1.1.0
```

## Architecture changes

Any change that touches one of these gets an Architecture Decision Record (ADR):

- The leader election model (lock TTL, role semantics)
- LSEG login model (warm/cold split)
- Kafka topic schema
- Database schema

Put ADRs in `docs/adr/NNN-title.md` using the [MADR](https://adr.github.io/madr/) format.
