# Architecture

## Table of contents

1. [The problem](#the-problem)
2. [The solution](#the-solution)
3. [Component breakdown](#component-breakdown)
4. [Key flows](#key-flows)
5. [Data stores](#data-stores)
6. [Trade-offs and design decisions](#trade-offs-and-design-decisions)
7. [Related diagrams](#related-diagrams)

---

## The problem

LSEG bills per **active EMA login** (one `OmmConsumer` instance = one billable session). Naive HA design with 4 OpenShift pods → 4 logins → 4× cost.

We need:
- 24×7 availability with fast failover (target: <10s)
- Subscription routing driven by business MS requests (not static config)
- Per-RIC tick gap detection so consumers know when data was missed
- Reasonable LSEG cost (target: 2 logins)

## The solution

A **leader election + warm-standby** pattern using Redis. Of 4 pods:

| Pod    | Role             | LSEG session              | Cost |
|--------|------------------|---------------------------|------|
| pod-0  | `warm-eligible`  | open + actively subscribed (leader) | login #1 |
| pod-1  | `warm-eligible`  | open + idle (no subscriptions) | login #2 |
| pod-2  | `cold-only`      | none unless promoted      | 0 |
| pod-3  | `cold-only`      | none unless promoted      | 0 |

**Total: 2 logins**, regardless of how many cold standbys we add for additional resilience.

On leader failure:
1. Redisson lock TTL expires (~5s)
2. pod-1 wins the new election (~1s)
3. pod-1 already has an EMA session open — just sends `ReqMsg` for all RICs in the registry (~1s)
4. LSEG sends snapshots; pod-1 emits `GAP_DETECTED` events on the control topic

**Worst-case downtime: 5–7 seconds.**

---

## Component breakdown

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            market-data-service                            │
│                                                                            │
│  ┌─────────────────────┐    ┌────────────────────┐    ┌────────────────┐ │
│  │ LeaderElection-     │    │ OmmConsumerManager │    │ SubscriptionMgr│ │
│  │  Service            │    │                    │    │                │ │
│  │  ▸ Redisson lock    │◄──►│  ▸ Owns OmmConsumer│◄──►│ ▸ Refcount     │ │
│  │  ▸ Heartbeat 2s     │    │  ▸ Subscribe/unsub │    │ ▸ Drain grace  │ │
│  │  ▸ Publishes events │    │  ▸ Mock mode flag  │    │ ▸ Tick listener│ │
│  └─────────────────────┘    └────────────────────┘    └────────┬───────┘ │
│         ▲                                                       │         │
│         │ PromotedToLeaderEvent / DemotedEvent                  │         │
│         │                                                       │         │
│  ┌──────┴─────────────┐    ┌───────────────────┐    ┌──────────▼───────┐ │
│  │ RecoveryService    │    │ MarketDataCallback│    │ MarketDataKafka- │ │
│  │  ▸ GapDetection    │    │  Handler          │    │  Producer        │ │
│  │  ▸ Emits           │    │  ▸ EMA OmmConsumer│    │  ▸ Transactional │ │
│  │    LEADER_CHANGE   │    │   Client          │    │  ▸ Partitioned   │ │
│  │  ▸ Per-RIC gap calc│    │  ▸ Tick decoding  │    │   by RIC         │ │
│  └────────────────────┘    └───────────────────┘    └──────────────────┘ │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │ Subscription Request Listener (Kafka consumer)                      │ │
│  │   ▸ Reads market-data-requests                                       │ │
│  │   ▸ Only leader acts; others skip                                    │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```

### Roles & responsibilities

| Component | Responsibility | Source |
|---|---|---|
| `LeaderElectionService` | Acquires/renews Redis lock; publishes `PromotedToLeaderEvent` / `DemotedEvent` | `leader/` |
| `PodRole` | Static role from env var: `WARM_ELIGIBLE` or `COLD_ONLY` | `leader/PodRole.java` |
| `OmmConsumerManager` | Owns the EMA `OmmConsumer`. Opens session for warm-eligible pods at startup; cold-only pods open lazily on promotion | `lseg/` |
| `MarketDataCallbackHandler` | Implements `OmmConsumerClient`; decodes `RefreshMsg`/`UpdateMsg` into `MarketDataTick` | `lseg/` |
| `LsegAuthService` | OAuth2 V2 token mgmt; caches token in Redis for diagnostics | `lseg/` |
| `SubscriptionManager` | Refcount-driven subscribe/unsubscribe; resubscribes on promotion | `subscription/` |
| `RefcountService` | Redisson `RAtomicLong` per RIC | `subscription/` |
| `RicRegistryService` | Durable registry in PostgreSQL + live set in Redis | `subscription/` |
| `SubscriptionRequestListener` | Kafka consumer for `market-data-requests` | `subscription/` |
| `MarketDataKafkaProducer` | Transactional producer to `market-data-updates` + `market-data-control` | `publisher/` |
| `GapDetectionService` | Per-RIC gap computation from `ric:last-published:*` | `recovery/` |
| `RecoveryService` | Orchestrates the post-promotion recovery flow | `recovery/` |
| `LsegConnectionHealthIndicator` | Used by readiness probe | `health/` |
| `MarketDataMetrics` | Custom Micrometer gauges | `metrics/` |

---

## Key flows

### 1. Subscribe flow

```
Business MS                                                     LSEG
   │                                                              │
   │ 1. publish {ric:"EUR=",action:SUBSCRIBE,requester:"trading"} │
   ▼                                                              │
Kafka (market-data-requests)                                      │
   │                                                              │
   │ 2. consume                                                   │
   ▼                                                              │
SubscriptionRequestListener                                       │
   │                                                              │
   │ 3. handleSubscribe()                                         │
   ▼                                                              │
SubscriptionManager ──► RicRegistryService ──► PG (registry, requests, audit)
                  │                                               │
                  │── ► RefcountService ──► Redis INCR refcount   │
                  │                                               │
                  └── ► OmmConsumerManager ──► EMA ReqMsg ────────►│
                                                                  │
                                              snapshot ◄──────────│ RefreshMsg
                                                                  │
                  ◄── MarketDataCallbackHandler                    │
                  │                                               │
                  ├── Redis cache (price, last-published)         │
                  └── KafkaProducer ──► market-data-updates       │
                                                                  ▼
                                                                Trading MS consumes
```

If a second business MS subscribes to the same RIC, refcount becomes 2 but **no new EMA call** — the cached snapshot is republished from Redis.

### 2. Unsubscribe + drain

```
Business MS publishes UNSUBSCRIBE
   │
   ▼
SubscriptionManager.handleUnsubscribe
   │
   ├── decrement refcount in Redis
   ├── if refcount == 0 → schedule drain (30s grace)
   │
   ▼ (after 30s)
If refcount still 0 → OmmConsumerManager.unsubscribe → CloseMsg to LSEG
                  → ric_registry.active = false
                  → emit RIC_UNSUBSCRIBED audit
```

The 30-second grace prevents thrash when a service restarts and resubscribes within a window.

### 3. Failover

See [the architecture diagram](../market-data-service-architecture.drawio) page 4 for the full sequence diagram.

```
t=0s   Pod 0 dies (OOM/eviction/network)
t=0s   LSEG detects TCP reset and closes the EMA stream from its side
t=0–5s Redis lock TTL counts down
t=5s   leader:lock auto-deleted
t=5s   Pod 1 (warm-eligible) wins SETNX
t=5s   onPromotedToLeader event fires
t=6s   Pod 1 reads ric_registry from PG (all active RICs)
t=6s   Pod 1 sends batch ReqMsg via its already-open OmmConsumer (no new login)
t=7s   LSEG sends snapshots
t=7s   Pod 1 emits GAP_DETECTED for each RIC (with gap_start = last-published ts)
t=7s   Pod 1 resumes publishing live ticks
```

### 4. OAuth2 token refresh

The EMA SDK manages its own internal token via `EnableSessionManagement=1` in `EmaConfig.xml`. The `LsegAuthService` separately fetches a token for:
- diagnostic visibility in Redis (`lseg:auth:token`)
- non-EMA REST calls (service discovery, reference lookups)

Tokens are refreshed proactively 5 minutes before expiry.

---

## Data stores

### Redis keys

| Key | Type | Purpose | TTL |
|---|---|---|---|
| `marketdata:leader:lock` | String | Distributed lock for leader election. Value = pod name. | 5s (renewed every 2s) |
| `marketdata:ric:active` | Sorted Set | Live set of subscribed RICs. Score = first-subscribed epoch ms. | — |
| `marketdata:ric:price:{ric}` | String (JSON) | Latest tick cache | 60s |
| `marketdata:ric:last-published:{ric}` | String | Epoch ms of last tick — used for gap detection | 60s |
| `marketdata:subscription:refcount:{ric}` | AtomicLong | Number of business MSs subscribed to this RIC | — |
| `marketdata:lseg:auth:token` | String (JSON) | Cached OAuth2 token (diagnostic) | token expiry − 60s |

### PostgreSQL tables

| Table | PK | Purpose |
|---|---|---|
| `ric_registry` | `ric` | Durable list of subscribed RICs + state |
| `subscription_requests` | `(business_ms, ric)` | Which MS asked for which RIC |
| `market_data_gaps` | `id` | Persistent log of detected gaps |
| `subscription_audit` | `id` | Append-only audit log of pod + RIC events |

See `src/main/resources/db/migration/V1__init.sql` for the full schema.

### Kafka topics

| Topic | Partitions | Direction | Key | Retention |
|---|---|---|---|---|
| `market-data-requests` | 6 | business MS → adapter | RIC | 7 days |
| `market-data-updates` | 12 | adapter → business MS | RIC | 24 hours |
| `market-data-control` | 3 | adapter → business MS | RIC or `*` | 30 days |

#### Leader-only consumption of `market-data-requests`

All 4 pods share the consumer group `market-data-adapter`, so by default Kafka would
distribute the 6 partitions across all pods — meaning non-leader pods would receive
subscription requests they can't act on.

To fix this, `SubscriptionRequestListener` declares its `@KafkaListener` with
`autoStartup = "false"` and starts/stops the listener container explicitly on
`PromotedToLeaderEvent` / `DemotedEvent`:

- Only the leader's listener is running → only the leader joins the consumer group
- The leader gets all 6 partitions
- Non-leader pods hold no Kafka subscription on this topic
- During failover, requests pile up in the topic (Kafka holds them) and the new leader
  consumes from its committed offset when its listener starts

Trade-off: requests experience an extra ~5s delay during failover (Kafka consumer group
rebalance time). That's acceptable for subscription requests — far less critical than
the tick stream itself.

---

## Trade-offs and design decisions

| Decision | Why | Alternative considered |
|---|---|---|
| **2 logins (warm standby)** vs 1 (cold) | 5–7s failover vs 30+s | Single login is cheaper but gives unacceptable downtime for a 24×7 trading feed |
| **Redis SET-NX** vs Redisson `RLock` | Simpler — we need to identify the holder and renew explicitly; `RLock` does this too but with more abstraction | Spring Integration LeaderInitiator (heavier; less control) |
| **5s lock TTL + 2s heartbeat** | Balance failover speed vs false positives from GC pauses | 30s/10s — safer but downtime too long |
| **Refcount in Redis, durable in PG** | Fast path + audit trail | Refcount only in PG — slower; only in Redis — no recovery on Redis flush |
| **30s drain grace** | Avoids thrash on subscriber restart | No grace — extra login churn |
| **Transactional Kafka producer** | Exactly-once tick delivery | At-least-once + idempotent consumers — pushes complexity to consumers |
| **Per-RIC gap timestamps in Redis** | O(1) gap detection on failover | Compute from Kafka offsets — more complex, slower |
| **StatefulSet** vs Deployment | Stable pod names → role assignment by ordinal | Deployment + label-based role — harder to make role sticky |
| **Cold-only pods 2 & 3** | Future scale-out + zero idle login cost | Just 2 pods — no scale headroom |

---

## Related diagrams

The companion file `market-data-service-architecture.drawio` (7 pages) covers:

1. System architecture (the picture from this doc, with all arrows)
2. Sequence — Subscribe flow
3. Sequence — Steady-state tick flow
4. Sequence — Failover with warm standby
5. Sequence — Unsubscribe + token refresh
6. Workflow — Subscription lifecycle state machine
7. Workflow — Pod state / leader election state machine
