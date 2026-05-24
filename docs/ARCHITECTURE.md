# Architecture

## Table of contents

1. [The problem](#the-problem)
2. [The solution — dual-cluster topology](#the-solution--dual-cluster-topology)
3. [DACS authentication model](#dacs-authentication-model)
4. [Component breakdown](#component-breakdown)
5. [Key flows](#key-flows)
6. [Data stores](#data-stores)
7. [Failure scenarios](#failure-scenarios)
8. [Trade-offs and design decisions](#trade-offs-and-design-decisions)
9. [Related diagrams](#related-diagrams)

---

## The problem

We need a 24×7 market data adapter that ingests OMM streams from **on-prem TREP / RTDS** and republishes ticks to Kafka for downstream business microservices. Constraints:

- **Two OpenShift clusters** in different data center halls (hall1 + hall2) for disaster-recovery isolation
- **2 pods per cluster** (= 4 pods total)
- **TREP DACS user licensing** — each `OmmConsumer` opens its own session against the DACS server and counts against the user's `MaxLogins`
- **Static egress IPs per hall**, whitelisted at TREP for DACS position-based permissioning
- **Sub-10s failover** for any single failure (pod, ADS, or DC)
- **Subscription routing** driven by business MS requests via Kafka, not static config

The naive design — every pod open its own EMA session — burns 4 sessions and would force the DACS user's `MaxLogins` to be set wastefully high. It also delivers duplicate data through ADS for every RIC.

## The solution — dual-cluster topology

A **cross-hall leader election** pattern using Enterprise Redis (active-active across both halls). Of the 4 pods:

| Pod | Hall | Role | LSEG state | DACS session? |
|---|---|---|---|---|
| `market-data-service-0` | hall1 | `warm-eligible` | active subscriber (when leader) | yes (#1) |
| `market-data-service-1` | hall1 | `cold-only` | none unless promoted | no |
| `market-data-service-0` | hall2 | `warm-eligible` | cross-hall warm standby | yes (#2) |
| `market-data-service-1` | hall2 | `cold-only` | none unless promoted | no |

**Total: 2 active DACS sessions**, against the same DACS named user.

### TREP / RTDS connection model

TREP is **not** split by hall. Both OpenShift clusters connect to the **same load balancer pair** in front of the TREP/RTDS infrastructure:

```
                    LSEG TREP / RTDS (single logical service)
                    ┌────────────────────────────────────────┐
                    │  DACS Server (auth + permissioning)    │
                    │  Load Balancer  ▸  trep-lb-1:14002     │
                    │                 ▸  trep-lb-2:14002     │
                    │  (may collapse to single IP if a       │
                    │   backend ADS fails)                   │
                    └────────────┬──────────────────┬────────┘
       DACS session #1 (active)  │                  │  DACS session #2 (warm)
       position=10.10.1.100      │                  │  position=10.10.2.100
                                 ▼                  ▼
       ┌──────────────────────────┐         ┌──────────────────────────┐
       │ Hall 1 OpenShift          │         │ Hall 2 OpenShift          │
       │  pod-0 🟢   pod-1 ⚪      │         │  pod-0 🟡   pod-1 ⚪      │
       │  egress 10.10.1.100      │         │  egress 10.10.2.100      │
       └─────────────┬────────────┘         └────────────┬─────────────┘
                     │                                    │
                     └──── Enterprise Redis (CRDT) ──────┘
                              (cross-hall lock + state)
                                       │
                                    Kafka  ◄──►  LCM MS
                                  (3 topics)
```

Both halls use the same `Channel_TREP_LB_Primary` + `Channel_TREP_LB_Backup` ChannelSet — what distinguishes them at DACS is the **source IP** (`position`).

## DACS authentication model

On-prem TREP uses **DACS** (Data Access Control System) for authentication, not OAuth2 (cloud RTO does). Each EMA `OmmConsumer.initialize()` sends a LOGIN domain message containing three identifiers:

| LOGIN attribute | Value | Configured via |
|---|---|---|
| `username` | `marketdata-adapter` (same for all pods) | `LSEG_DACS_USERNAME` secret |
| `position` | static hall egress IP — `10.10.1.100` or `10.10.2.100` | `LSEG_DACS_POSITION` ConfigMap |
| `applicationId` | `256` | `LSEG_DACS_APPLICATION_ID` ConfigMap |

The DACS server then verifies:
1. User exists and password is correct
2. Position IP is on the user's allowed-positions list (TREP team whitelists both hall egress IPs)
3. The user is below their `MaxLogins` ceiling
4. The user is entitled to the products being subscribed

> Full detail and cost analysis: **[DACS_LICENSING.md](DACS_LICENSING.md)**.

### TREP LB endpoint failover (no new login)

Each pod's `OmmConsumer` is configured with a `ChannelSet` of the two TREP load-balancer endpoints:

```xml
<ChannelSet value="Channel_TREP_LB_Primary, Channel_TREP_LB_Backup"/>
```

If LB endpoint #1 stops accepting connections (either the LB removed it because the backend ADS failed, or the LB itself is restarting), EMA automatically reconnects to LB endpoint #2 and **replays the LOGIN domain on the new channel using the same credentials** — no new DACS session is opened. Both halls use this same configuration; the only per-hall difference is the source IP (`position`).

### Cross-hall failover (application-level)

We deliberately do **not** use EMA `WarmStandbyChannelSet` to connect to both halls' ADS from a single pod. That would double our DACS session count. Instead, cross-hall HA is handled at the application level:
- hall1/pod-0 has its own session to ADS-hall1
- hall2/pod-0 has its own session to ADS-hall2
- Only one of them is "active leader" at any time, via Redis lock

---

## Component breakdown

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          market-data-adapter (per pod)                    │
│                                                                            │
│  ┌─────────────────────┐    ┌────────────────────┐    ┌────────────────┐ │
│  │ LeaderElection-     │    │ OmmConsumerManager │    │ SubscriptionMgr│ │
│  │  Service            │    │  ▸ DACS or OAuth2  │    │                │ │
│  │  ▸ Cross-hall Redis │◄──►│  ▸ ChannelSet      │◄──►│ ▸ Refcount     │ │
│  │  ▸ Lock holder ID = │    │    (ADS pri+bkup)  │    │ ▸ Drain grace  │ │
│  │    "hallN/pod-name" │    │  ▸ position = IP   │    │ ▸ Tick listener│ │
│  └─────────────────────┘    └────────────────────┘    └────────┬───────┘ │
│         ▲                                                       │         │
│         │ PromotedToLeaderEvent / DemotedEvent                  │         │
│         │                                                       │         │
│  ┌──────┴─────────────┐    ┌───────────────────┐    ┌──────────▼───────┐ │
│  │ RecoveryService    │    │ MarketDataCallback│    │ MarketDataKafka- │ │
│  │  ▸ GapDetection    │    │  Handler          │    │  Producer        │ │
│  │  ▸ Cross-hall      │    │  ▸ OmmConsumer-   │    │  ▸ Transactional │ │
│  │    failover events │    │    Client         │    │  ▸ Partitioned   │ │
│  └────────────────────┘    └───────────────────┘    └──────────────────┘ │
│                                                                            │
│  Subscription Request Listener — only running when this pod is leader     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Roles & responsibilities

| Component | Responsibility | Source |
|---|---|---|
| `LeaderElectionService` | Acquires/renews shared cross-hall Redis lock. Lock value = `hallN/pod-name` for visibility. | `leader/` |
| `PodRole` | Static role from `POD_ROLE` env var: `WARM_ELIGIBLE` or `COLD_ONLY`. | `leader/PodRole.java` |
| `MarketDataProperties.Pod.hall` | Identifies which OpenShift cluster the pod runs in. Used in audit/metrics. | `config/` |
| `OmmConsumerManager` | Owns the singleton `OmmConsumer`. Configures DACS (on-prem) or OAuth2 (RTO) based on `connection-mode`. Both halls connect to the same TREP LB endpoints; only `position` differs. | `lseg/` |
| `MarketDataCallbackHandler` | Implements `OmmConsumerClient`; decodes EMA messages into `MarketDataTick`. | `lseg/` |
| `LsegAuthService` | OAuth2 token mgmt — **no-op on `ON_PREM_TREP`**. DACS handles auth inline. | `lseg/` |
| `SubscriptionManager` | Refcount-driven subscribe/unsubscribe. Resubscribes on promotion. | `subscription/` |
| `RefcountService` | Redisson `RAtomicLong` per RIC. | `subscription/` |
| `RicRegistryService` | Durable registry in PostgreSQL + live set in Redis. | `subscription/` |
| `SubscriptionRequestListener` | Kafka consumer started/stopped on leader transitions. | `subscription/` |
| `MarketDataKafkaProducer` | Transactional producer to `market-data-updates` + `market-data-control`. | `publisher/` |
| `GapDetectionService` | Per-RIC gap from `ric:last-published:*`. | `recovery/` |
| `RecoveryService` | Post-promotion recovery orchestration. Emits cross-hall failover events. | `recovery/` |
| `LsegConnectionHealthIndicator` | Readiness probe. | `health/` |

---

## Key flows

### TREP LB endpoint failover

```
Pod-0 (leader)  ─── EMA session #1 (DACS user, position=10.10.1.100) ───►  trep-lb-1:14002
                                                                                  │
                                            LB endpoint #1 unavailable ◄─────────┘
                                                ▼ (EMA detects disconnect)
                EMA ChannelSet reconnect attempt ────►  trep-lb-2:14002
                                                                  │
                LOGIN replayed automatically (same DACS user + position) ─◄┘
                ◄── subscriptions restored, ticks resume                   ╱
                                                                          ╱
                NO NEW DACS SESSION OPENED ───────────────────────────────┘
```

~2–5s blackout. No LCM-MS action required; downstream sees a brief gap that may or may not exceed the gap-detection threshold.

### Cross-hall failover (catastrophic hall1 outage)

See **Page 8** of the architecture diagram for the full sequence. Summary:

```
t=0s    hall1 catastrophic failure (DC down)
t=0s    TREP-LB connection from hall1 drops (egress 10.10.1.100 unreachable)
t=0-5s  Redis lock TTL counts down (5s)
t=5s    leader:lock auto-deleted
t=5s    hall2/pod-0 wins SETNX (already has DACS session #2 from egress 10.10.2.100)
t=6s    hall2/pod-0 reads ric_registry from PostgreSQL
t=6s    Batch ReqMsg for all RICs via existing OmmConsumer
t=7s    LSEG sends snapshots; GAP_DETECTED events emitted on market-data-control
t=7s    Tick publication resumes from hall2/pod-0
```

**Total downtime: ~5–7s.**

### Subscribe flow

Unchanged from single-cluster design — see **Page 2** of the architecture diagram. Only the active leader (whichever hall) consumes from `market-data-requests`.

---

## Data stores

### Redis keys (Enterprise Redis — cross-hall)

| Key | Type | Purpose | TTL |
|---|---|---|---|
| `marketdata:leader:lock` | String | Cross-hall lock. Value = `hallN/pod-name`. | 5s (renewed every 2s) |
| `marketdata:ric:active` | Sorted Set | Live set of subscribed RICs. | — |
| `marketdata:ric:price:{ric}` | String (JSON) | Latest tick cache. | 60s |
| `marketdata:ric:last-published:{ric}` | String | Epoch ms of last tick — gap detection. | 60s |
| `marketdata:subscription:refcount:{ric}` | AtomicLong | Number of business MSs subscribed. | — |

> All keys are **global** across both halls. Enterprise Redis replicates the keyspace; CRDT semantics handle simultaneous writes during a partition.

### PostgreSQL tables (cross-hall HA)

Same schema as before — see `src/main/resources/db/migration/V1__init.sql`. The DB itself should be deployed with cross-hall replication (e.g., Crunchy Postgres with synchronous standby in hall2).

### Kafka topics (multi-DC)

| Topic | Partitions | Direction | Key | Retention |
|---|---|---|---|---|
| `market-data-requests` | 6 | business MS → adapter | RIC | 7d |
| `market-data-updates` | 12 | adapter → business MS | RIC | 24h |
| `market-data-control` | 3 | adapter → business MS | RIC or `*` | 30d |

Kafka should be deployed multi-DC (Strimzi MirrorMaker, Confluent Replicator, or a stretched cluster).

### Leader-only consumption of `market-data-requests`

The `SubscriptionRequestListener` `@KafkaListener` is registered with `autoStartup = false`. On `PromotedToLeaderEvent` it starts the container; on `DemotedEvent` it stops. Only the active leader (in either hall) consumes — Kafka assigns all 6 partitions to it. Trade-off: ~5–10s request lag during failover while the new leader's consumer joins the group.

---

## Failure scenarios

| Scenario | Mechanism | Downtime | Cost / capacity impact |
|---|---|---|---|
| TREP LB drops one IP (backend ADS dies) | EMA `ChannelSet` reconnects to the surviving LB IP, same DACS session | 2–5s | none |
| Active leader pod crashes | Cold-only pod in same hall promotes; opens new DACS session | 10–15s | brief 2-session window; OK with `MaxLogins ≥ 3` |
| Full hall outage (DC down) | hall2/pod-0 (warm standby) wins lock; already has DACS session #2 | 5–7s | none — uses pre-existing session |
| Redis becomes unreachable | All pods enter `COMPETING` state; no leader; subscriptions stall but EMA sessions stay open | until Redis recovers | none new |
| ADS user `MaxLogins` exceeded | DACS rejects new logins; only existing sessions work | depends on which session | operational alert |
| Network partition between halls | Each hall sees the other as down; only the side that retains Redis quorum proceeds | hall on minority side stalls | none |

---

## Trade-offs and design decisions

| Decision | Why | Alternative considered |
|---|---|---|
| **2 DACS sessions (1 active + 1 cross-hall warm)** | Survives DC failure in ~5–7s | 1 session (cold-only everywhere) → ~10–15s + DC blast radius |
| **Same DACS user across all pods** | Per-user licensing → 1 user fee regardless of pod count | Different user per pod → 4× licensing cost |
| **No EMA `WarmStandbyChannelSet`** | Would open 2 connections per pod → 4 sessions total | Accepted: app-level cross-hall failover instead |
| **Cross-hall lock holder ID includes hall name** | Easier debugging — `redis-cli get leader:lock` shows which hall holds it | Just pod name — ambiguous |
| **`ChannelSet` within hall (primary + backup ADS)** | Transparent recovery from ADS failure, no new login | Single ADS per channel — full reconnect needed on ADS failure |
| **5s lock TTL, 2s heartbeat** | Failover within 7s; tolerates GC pauses up to ~3s | 30s TTL → too slow; 1s TTL → false failovers |
| **Cold-only pods (1 per hall)** | Cheap insurance against warm pod failure | Skip them → faster reaction, but no within-hall headroom |
| **Refcount in Redis, durable in PG** | Fast path + audit trail | PG-only → slow; Redis-only → no recovery on flush |
| **30s drain grace** | Avoids thrash on subscriber restart | No grace → wasted ADS subscription churn |

---

## Related diagrams

The companion file `market-data-service-architecture.drawio` (8 pages):

1. **System Architecture (dual-hall)** — the picture from this doc, with all arrows
2. Sequence — Subscribe flow
3. Sequence — Steady-state tick flow
4. Sequence — Within-hall failover
5. Sequence — Unsubscribe + token refresh
6. Workflow — Subscription lifecycle state machine
7. Workflow — Pod state / leader election state machine
8. **Sequence — Cross-Hall Failover** — hall1 catastrophic outage → hall2 warm standby promotion
