# market-data-adapter

> LSEG Real-Time market data adapter — a Spring Boot microservice that ingests OMM streams from **on-prem TREP / RTDS** and republishes them to Kafka for downstream business services.

[![Java](https://img.shields.io/badge/Java-21-blue)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.4-green)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/license-internal-lightgrey)]()

---

## Why this exists

LSEG TREP licensing is **per DACS named user**, but each open EMA session counts against that user's `MaxLogins` and consumes ADS resources. With 4 pods across 2 OpenShift clusters (`hall1` + `hall2`), naive deployment = 4 concurrent DACS sessions. This service uses a **cross-hall leader election** pattern to keep exactly **2 active DACS sessions** (1 active subscriber + 1 cross-hall warm standby) for ~5–7s failover.

```
                       LSEG TREP / RTDS (on-prem)
                              ▲ ▲
              DACS session #1 │ │ DACS session #2 (warm)
       (same DACS user, diff position IP per hall)
                              │ │
       ┌──────────────────────┼─┼──────────────────────┐
       │                      ▼ ▼                       │
   ┌────────────┐                         ┌────────────┐
   │  Hall 1    │  egress: 10.10.1.100    │  Hall 2    │  egress: 10.10.2.100
   │  pod-0 🟢  │  pod-1 ⚪               │  pod-0 🟡  │  pod-1 ⚪
   └─────┬──────┘                         └──────┬─────┘
         │                                       │
         └────── Enterprise Redis ───────────────┘
                (cross-hall lock + state)
                          │
                       Kafka ──► LCM MS
```

---

## Quick links

| Doc | What it covers |
|---|---|
| **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** | Dual-hall topology, DACS session model, key flows |
| **[docs/DACS_LICENSING.md](docs/DACS_LICENSING.md)** | Per-DACS-user licensing — why we get away with 1 user license |
| **[docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md)** | Run the full stack on your laptop in 5 minutes |
| **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** | Every env var and `marketdata.*` property |
| **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** | Two-cluster OpenShift deploy + failover testing |
| **[docs/OPERATIONS.md](docs/OPERATIONS.md)** | Health, metrics, alerts, runbooks |
| **[docs/TESTING.md](docs/TESTING.md)** | Unit + Testcontainers integration tests |
| **[market-data-service-architecture.drawio](market-data-service-architecture.drawio)** | 8-page draw.io: dual-hall topology + sequences + state machines |

---

## Pod role matrix

| Pod | Cluster | Role | Active DACS session | What it does |
|---|---|---|---|---|
| `market-data-service-0` | hall1 | `warm-eligible` | yes (session #1) | currently active leader / subscriber |
| `market-data-service-1` | hall1 | `cold-only` | no | within-hall standby; opens session only if promoted |
| `market-data-service-0` | hall2 | `warm-eligible` | yes (session #2) | cross-hall warm standby — open EMA session, no subs |
| `market-data-service-1` | hall2 | `cold-only` | no | last-resort capacity |

**Total: 1 DACS named user × 2 concurrent sessions.**

---

## Failover characteristics

| Scenario | Mechanism | Downtime | Cost impact |
|---|---|---|---|
| TREP LB drops one IP (backend ADS fails) | EMA `ChannelSet` reconnects to surviving LB IP | ~2–5s | none (same DACS session) |
| Active leader pod crashes (within hall) | Cold standby promotes; opens new DACS session | ~10–15s | brief 2-session window |
| Full hall1 outage (DC failure) | Hall2 warm standby (`hall2/pod-0`) wins lock | ~5–7s | none (uses existing session #2) |
| Both warm pods unavailable | A cold-only pod cold-starts | ~10–15s | brief 2-session window |

---

## Five-minute quickstart (local dev with mock LSEG)

```bash
# 1. Start infra
docker compose -f docker/docker-compose.yml up -d

# 2. Build the app image
docker build -t market-data-adapter:dev .

# 3. Run two pods to simulate hall1 + hall2
docker run -d --name md-hall1-pod0 --network docker_default \
  -e SPRING_PROFILES_ACTIVE=local -e HALL=hall1 -e POD_NAME=local-hall1-pod-0 \
  -e POD_ROLE=warm-eligible -e LSEG_MOCK=true \
  -e DB_URL=jdbc:postgresql://marketdata-postgres:5432/marketdata \
  -e REDIS_HOST=marketdata-redis -e KAFKA_BOOTSTRAP=marketdata-kafka:29092 \
  -p 8080:8080 market-data-adapter:dev

docker run -d --name md-hall2-pod0 --network docker_default \
  -e SPRING_PROFILES_ACTIVE=local -e HALL=hall2 -e POD_NAME=local-hall2-pod-0 \
  -e POD_ROLE=warm-eligible -e LSEG_MOCK=true \
  -e DB_URL=jdbc:postgresql://marketdata-postgres:5432/marketdata \
  -e REDIS_HOST=marketdata-redis -e KAFKA_BOOTSTRAP=marketdata-kafka:29092 \
  -p 8081:8080 market-data-adapter:dev

# 4. Verify
curl -s http://localhost:8080/actuator/prometheus | grep marketdata_leader
curl -s http://localhost:8081/actuator/prometheus | grep marketdata_leader
# One should be 1.0 (leader); the other 0.0 (warm standby)
```

---

## Status

- ✅ Compiles + image builds cleanly
- ✅ Leader election works across containers (within-cluster verified; cross-cluster requires Enterprise Redis)
- ✅ Kafka pipeline works end-to-end (subscribe → tick → publish, mock LSEG)
- ⚠ Real TREP connection requires DACS username + whitelisted egress IPs from the TREP team
- ⚠ Integration tests need Docker running for Testcontainers

---

## License

Internal — see your organization's licensing policy.
