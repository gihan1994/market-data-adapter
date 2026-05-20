# Testing

## Test types

| Type | Where | What it covers |
|---|---|---|
| Unit tests | `src/test/java/.../<package>/` | Pure logic — refcount math, gap computation, DTO mapping |
| Integration tests | Same — extend `AbstractIntegrationTest` | Spring context + real Postgres/Redis/Kafka via Testcontainers |

We don't run real LSEG in tests — `LSEG_MOCK=true` is set in `application-test.yml`, so `OmmConsumerManager` doesn't open a real EMA session.

## Prerequisites

- Docker Desktop running (Testcontainers needs the Docker daemon)
- Maven 3.9+ and JDK 21

## Run all tests

```bash
mvn verify
```

Or just the unit + integration test phase:

```bash
mvn test
```

> Note: `mvn test` and `mvn verify` are equivalent here because we don't separate `*IT.java` integration tests into the failsafe phase yet. All tests run under surefire.

## Run a single test

```bash
mvn test -Dtest=RefcountServiceTest
mvn test -Dtest=LeaderElectionServiceTest#singlePodEventuallyBecomesLeader
```

## Test catalog

### `MarketDataServiceApplicationTests`
Just loads the Spring context. Catches DI/bean-wiring breakage early.

### `RefcountServiceTest`
- Increment and decrement reach 0 cleanly
- Decrement past 0 is clamped (never negative)
- Reset clears the key

### `LeaderElectionServiceTest`
- Single pod eventually becomes `LEADER` (within 10s)
- Final state matches `PodRole.WARM_ELIGIBLE`

### `SubscriptionManagerTest`
- First subscriber triggers `OmmConsumerManager.subscribe()`
- Second subscriber to the same RIC increments refcount but doesn't double-call EMA
- Unsubscribing all subscribers triggers drain → EMA unsubscribe after the grace period

### `GapDetectionServiceTest`
- Detects a gap when `ric:last-published` > threshold
- No gap when within threshold
- No gap when there's no prior timestamp (fresh subscription)

### `MarketDataKafkaProducerTest`
- A tick published via the transactional producer is consumed by a separate consumer
- Bid/ask are decoded correctly on the consumer side

## How tests work

`AbstractIntegrationTest` spins up Testcontainers in a static block:

```java
static final PostgreSQLContainer<?> POSTGRES = ...;
static final RedisContainer       REDIS    = ...;
static final KafkaContainer       KAFKA    = ...;

static { POSTGRES.start(); REDIS.start(); KAFKA.start(); }

@DynamicPropertySource
static void registerProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url",      POSTGRES::getJdbcUrl);
    registry.add("redis.host",                 REDIS::getHost);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    // ...
}
```

Containers are reused across test classes (`.withReuse(true)`), which dramatically speeds up the suite. To use reuse on a CI box, ensure `~/.testcontainers.properties` contains `testcontainers.reuse.enable=true`.

## CI integration

For GitHub Actions / Jenkins / Tekton, ensure the runner has Docker available:

```yaml
# GitHub Actions example
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven
      - run: mvn -B verify
```

In OpenShift Pipelines (Tekton), use a task that includes a Docker daemon sidecar.

## Adding a new test

```java
package com.example.marketdata.subscription;

import com.example.marketdata.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

class MyNewTest extends AbstractIntegrationTest {

    @Autowired SomeService someService;

    @Test
    void myAssertion() {
        assertThat(someService.compute()).isEqualTo(42);
    }
}
```

`AbstractIntegrationTest` already brings up the Spring context with profile `test`. Just `@Autowired` the bean you need.

## Common test failures

### `Could not find a valid Docker environment`

Docker Desktop isn't running. Start it.

### Tests pass locally but fail in CI

Check the CI runner has Docker socket access. Some runners (e.g. Kubernetes-based) need a Docker-in-Docker sidecar or `socat` to forward the host's Docker socket.

### `Cannot connect to Kafka` after KafkaContainer starts

Kafka takes 10–15s to be fully ready. Most tests `await()` for the app to be ready, but if you wrote a test that doesn't, add an Awaitility wait.

### `Already exists` errors on the second test run

Likely a Postgres unique-constraint violation because the previous run left rows behind (the container is reused but the Spring context is rolled back). Use unique RIC names per test:

```java
String ric = "TEST-" + System.nanoTime();
```
