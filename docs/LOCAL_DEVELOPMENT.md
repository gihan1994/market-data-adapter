# Local Development

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | 4.20+ | Required — runs the app, infra, and tests |
| Java JDK | 21 | Only if running `mvn` outside Docker |
| Maven | 3.9+ | Only if running tests/builds outside Docker |
| Git | any | |

Everything below works **without** local Java/Maven if you only use Docker.

---

## Building

### Option 1 — Docker (simplest, no JDK needed)

```bash
docker build -t market-data-service:dev .
```

The build stage uses `maven:3.9.9-eclipse-temurin-21-alpine`, so dependencies (including the LSEG EMA SDK) are pulled into the build image. First build takes ~5 min; subsequent builds use Maven's local cache layer (~30s).

### Option 2 — Local Maven

```bash
mvn clean package -DskipTests
```

Produces `target/market-data-service.jar`. Run with:

```bash
java -jar target/market-data-service.jar --spring.profiles.active=local
```

---

## Running

### 1. Start infra

```bash
docker compose -f docker/docker-compose.yml up -d
```

This brings up:
- `marketdata-postgres` on `localhost:5432` (user `marketdata`, password `marketdata`, db `marketdata`)
- `marketdata-redis` on `localhost:6379`
- `marketdata-kafka` on `localhost:9092`

Confirm:
```bash
docker compose -f docker/docker-compose.yml ps
```

### 2. Start the app

#### As a Docker container (recommended)

```bash
docker run -d --name marketdata-app --network docker_default \
  -e SPRING_PROFILES_ACTIVE=local \
  -e DB_URL=jdbc:postgresql://marketdata-postgres:5432/marketdata \
  -e REDIS_HOST=marketdata-redis \
  -e KAFKA_BOOTSTRAP=marketdata-kafka:29092 \
  -e POD_ROLE=warm-eligible \
  -e POD_NAME=local-pod-0 \
  -e LSEG_MOCK=true \
  -p 8080:8080 \
  market-data-service:dev
```

> ⚠ **Network name** — Docker Compose names the network after the directory containing the compose file. Our file is in `docker/`, so the network is `docker_default`, **not** `market-data-service_default`. Use `docker network ls` to confirm.

#### As a local JVM process

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

Defaults to `localhost` for all infra — works if infra is exposed on host ports (which `docker-compose.yml` does).

### 3. Verify it's running

```bash
curl http://localhost:8080/actuator/health
# {"status":"UP","groups":["liveness","readiness"]}

curl http://localhost:8080/actuator/prometheus | grep marketdata_
# marketdata_leader{...} 1.0      ← this pod is the leader
# marketdata_subscriptions_active 0.0
# ...
```

Check logs:
```bash
docker logs -f marketdata-app
```

You should see:
```
State transition: COMPETING → LEADER (lock acquired)
Promoted to leader — resubscribing all active RICs
Resubscribing 0 active RICs
```

### 4. Send a test subscribe request

```bash
echo 'EUR=:{"ric":"EUR=","action":"SUBSCRIBE","requester":"trading-ms"}' \
  | docker exec -i marketdata-kafka kafka-console-producer \
      --bootstrap-server localhost:9092 \
      --topic market-data-requests \
      --property "parse.key=true" --property "key.separator=:"
```

In the app logs:
```
recordSubscribe ric=EUR= requester=trading-ms totalActiveSubscribers=1 firstSubscriber=true
refcount[EUR=] ++ -> 1
[mock] subscribed EUR=
```

In Postgres:
```bash
docker exec marketdata-postgres psql -U marketdata -d marketdata \
  -c "SELECT * FROM ric_registry;"
```

---

## Running multiple pods (failover demo)

Start pod-1 (warm standby — different host port):
```bash
docker run -d --name marketdata-app-1 --network docker_default \
  -e SPRING_PROFILES_ACTIVE=local \
  -e DB_URL=jdbc:postgresql://marketdata-postgres:5432/marketdata \
  -e REDIS_HOST=marketdata-redis \
  -e KAFKA_BOOTSTRAP=marketdata-kafka:29092 \
  -e POD_ROLE=warm-eligible -e POD_NAME=local-pod-1 \
  -e LSEG_MOCK=true \
  -p 8081:8080 \
  market-data-service:dev
```

Confirm pod-1 is in standby:
```bash
curl http://localhost:8081/actuator/prometheus | grep marketdata_leader
# marketdata_leader{...,pod="local-pod-1"} 0.0
```

Kill pod-0 and watch pod-1 take over:
```bash
docker rm -f marketdata-app
sleep 8
curl http://localhost:8081/actuator/prometheus | grep marketdata_leader
# marketdata_leader{...,pod="local-pod-1"} 1.0
```

---

## Common issues & fixes

These all happened during the initial setup — documenting so you don't have to rediscover.

### `mvn` not found

Maven isn't installed. Either:
- Install via `choco install maven` / `scoop install maven`, or
- Use the Docker build path (no local Maven needed)

### `Could not connect to server` on `localhost:8080`

The app probably crashed on startup. Check:
```bash
docker logs marketdata-app --tail 60
```

### `Connection to localhost:5432 refused`

Inside the container, `localhost` is the container itself, not the host. The compose-network hostnames are `marketdata-postgres`, `marketdata-redis`, `marketdata-kafka` — pass them via env vars (see step 2 above).

### `LoggerFactory is not a Logback LoggerContext`

The LSEG `ema` jar transitively pulls in `slf4j-jdk14` (the JUL binding) which fights Logback.

> **Already fixed in `pom.xml`** via `<exclusion>` on `org.slf4j:slf4j-jdk14`.

### `The bean 'subscriptionManager' could not be injected because it is a JDK dynamic proxy`

Spring wraps `@Async` beans in a JDK proxy by default, which only implements interfaces — so `SubscriptionManager` (implements `TickListener`) gets proxied as `TickListener`, not as itself.

> **Already fixed** with `@EnableAsync(proxyTargetClass = true)` in `MarketDataServiceApplication.java`.

### `No bean named 'transactionManager' available`

When you declare a `KafkaTransactionManager` bean, Spring Boot's auto-config skips creating the JPA `transactionManager`. JPA repositories then can't find one.

> **Already fixed** in `KafkaConfig.java` — explicit `@Primary` JPA `transactionManager` bean.

### Tests fail with "Cannot connect to Docker daemon"

Testcontainers needs Docker running. Start Docker Desktop first.

---

## Tearing down

```bash
docker rm -f marketdata-app marketdata-app-1
docker compose -f docker/docker-compose.yml down -v   # -v drops volumes too
```
