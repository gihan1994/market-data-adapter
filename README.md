# market-data-service

> LSEG Real-Time market data adapter — a Spring Boot microservice that ingests OMM streams via the [LSEG Real-Time SDK](https://github.com/Refinitiv/Real-Time-SDK) and republishes them to Kafka for downstream business services.

[![Java](https://img.shields.io/badge/Java-21-blue)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.4-green)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/license-internal-lightgrey)]()

---

## Why this exists

LSEG charges **per active EMA login**. Running 4 naive pods on OpenShift means 4 logins (4× cost). This service uses a **leader election + warm-standby** pattern to keep exactly **2 logins** alive — one actively subscribing, one idle but pre-authenticated — for ~5–7 second failover instead of 30+ seconds.

```
┌─────────┐    1 login (active)     ┌─────────────┐
│  LSEG   │ ◄─────────────────────► │ Pod 0 LEADER│ ──┐
│   ERT   │    1 login (warm)       │ Pod 1 WARM  │ ──┼─► Kafka ──► business MSs
│         │ ◄─────────────────────► │ Pod 2 COLD  │   │
└─────────┘                         │ Pod 3 COLD  │ ──┘
                                    └─────────────┘
```

---

## Quick links

| Doc | What it covers |
|---|---|
| **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** | System design, the warm-standby model, key flows, data stores |
| **[docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md)** | Run the full stack on your laptop in 5 minutes |
| **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** | Every env var and `marketdata.*` property |
| **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** | OpenShift deploy + failover testing |
| **[docs/OPERATIONS.md](docs/OPERATIONS.md)** | Health, metrics, alerts, runbooks |
| **[docs/TESTING.md](docs/TESTING.md)** | Unit tests, Testcontainers integration tests |
| **[market-data-service-architecture.drawio](../market-data-service-architecture.drawio)** | 7-page draw.io: architecture + sequence diagrams + state machines |

---

## Five-minute quickstart

```bash
# 1. Start infra (Postgres, Redis, Kafka)
docker compose -f docker/docker-compose.yml up -d

# 2. Build the app image
docker build -t market-data-service:dev .

# 3. Run the app
docker run -d --name marketdata-app --network docker_default \
  -e SPRING_PROFILES_ACTIVE=local \
  -e DB_URL=jdbc:postgresql://marketdata-postgres:5432/marketdata \
  -e REDIS_HOST=marketdata-redis \
  -e KAFKA_BOOTSTRAP=marketdata-kafka:29092 \
  -e POD_ROLE=warm-eligible -e POD_NAME=local-pod-0 \
  -e LSEG_MOCK=true \
  -p 8080:8080 market-data-service:dev

# 4. Verify
curl http://localhost:8080/actuator/health
curl http://localhost:8080/actuator/prometheus | grep marketdata_

# 5. Send a test subscribe
echo 'EUR=:{"ric":"EUR=","action":"SUBSCRIBE","requester":"trading-ms"}' \
  | docker exec -i marketdata-kafka kafka-console-producer \
      --bootstrap-server localhost:9092 --topic market-data-requests \
      --property "parse.key=true" --property "key.separator=:"
```

Full walkthrough in **[docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md)**.

---

## High-level architecture

```
                    LSEG Real-Time Platform
                      OAuth2 + ERT/RTO
                            ▲ ▲
              login #1 ────┘ └──── login #2 (warm)
                    │              │
   ┌────────────────┼──────────────┼────────────────┐
   │ OpenShift (4 pods, StatefulSet)                │
   │  ┌──────────┐ ┌──────────┐ ┌──────┐ ┌──────┐  │
   │  │ Pod 0    │ │ Pod 1    │ │Pod 2 │ │Pod 3 │  │
   │  │ LEADER   │ │ WARM     │ │ COLD │ │ COLD │  │
   │  └────┬─────┘ └──────────┘ └──────┘ └──────┘  │
   └───────┼────────────────────────────────────────┘
           │
           ├──► Redis (leader lock, RIC refcount, price cache, gap ts)
           ├──► PostgreSQL (durable RIC registry, audit, gap log)
           └──► Kafka
                  market-data-requests  ◄── business MSs (subscribe/unsubscribe)
                  market-data-updates   ──► business MSs (live ticks)
                  market-data-control   ──► business MSs (gap + recovery events)
```

Detail on every piece: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

---

## Project layout

```
market-data-service/
├── pom.xml
├── Dockerfile
├── README.md                    ← you are here
├── docs/                        ← markdown documentation
├── docker/docker-compose.yml    ← local Postgres + Redis + Kafka
├── deploy/openshift/            ← StatefulSet, ConfigMap, Secrets, PDB
└── src/
    ├── main/java/com/example/marketdata/
    │   ├── config/        Beans for Redisson, Kafka, JPA, props
    │   ├── domain/        JPA entities
    │   ├── repository/    Spring Data repositories
    │   ├── leader/        LeaderElectionService, role enums, events
    │   ├── lseg/          OmmConsumerManager, callback handler, auth
    │   ├── subscription/  Refcount + registry + Kafka request listener
    │   ├── publisher/     Transactional Kafka producer + event DTOs
    │   ├── recovery/      Gap detection + recovery orchestration
    │   ├── health/        Actuator health indicators
    │   └── metrics/       Micrometer gauges
    ├── main/resources/    EmaConfig.xml, application*.yml, logback, migrations
    └── test/              Testcontainers integration tests
```

---

## Status

- ✅ Compiles + image builds cleanly
- ✅ Starts on local Docker; leader election works; Kafka pipeline works end-to-end (verified with mock LSEG)
- ⚠ Requires real LSEG credentials to test against live ERT — see [docs/CONFIGURATION.md#lseg-credentials](docs/CONFIGURATION.md#lseg-credentials)
- ⚠ Integration tests need Docker running for Testcontainers — see [docs/TESTING.md](docs/TESTING.md)

---

## License

Internal — see your organization's licensing policy.
