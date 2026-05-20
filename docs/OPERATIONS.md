# Operations

## Health endpoints

| Endpoint | Use |
|---|---|
| `GET /actuator/health` | Overall — used by both liveness and readiness probes |
| `GET /actuator/health/liveness` | Kubernetes liveness — `UP` while JVM is healthy |
| `GET /actuator/health/readiness` | Kubernetes readiness — `UP` only when LSEG session is open (for warm pods) |
| `GET /actuator/health/lseg` | LSEG connection state + active subscription count |
| `GET /actuator/health/leader` | Leader state, pod role, isLeader flag |

For details to show in the response, set `HEALTH_SHOW_DETAILS=always` (default is `when-authorized`).

---

## Metrics

All metrics are tagged `application=market-data-service` and `pod=<pod-name>`.

### Service-specific gauges

| Metric | Type | Description |
|---|---|---|
| `marketdata_leader` | gauge | `1` if this pod is the active leader, `0` otherwise |
| `marketdata_subscriptions_active` | gauge | RICs currently subscribed via EMA on this pod |
| `marketdata_subscriptions_registry` | gauge | Active RICs in the PG registry |
| `marketdata_kafka_ticks_published_total` | counter | Total ticks published |
| `marketdata_kafka_gaps_published_total` | counter | Total gap/control events published |
| `marketdata_kafka_publish_failures_total` | counter | Kafka send failures |
| `marketdata_kafka_publish_latency_seconds` | summary | Send latency (with percentiles) |

### Standard Spring/JVM metrics also available

- `jvm_memory_used_bytes`
- `jvm_gc_pause_seconds`
- `http_server_requests_seconds`
- `hikaricp_connections_active`
- `kafka_consumer_records_consumed_total`
- ... and many more — see `/actuator/prometheus`

---

## Recommended alerts

Put these in your Prometheus alerting rules. Adjust thresholds per environment.

```yaml
groups:
  - name: market-data-service
    rules:
      - alert: NoLeader
        expr: sum(marketdata_leader{application="market-data-service"}) == 0
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "market-data-service has no active leader"
          description: "All pods report marketdata_leader=0. Subscriptions are stalled."

      - alert: MultipleLeaders
        expr: sum(marketdata_leader{application="market-data-service"}) > 1
        for: 30s
        labels: { severity: critical }
        annotations:
          summary: "market-data-service split-brain — multiple leaders"
          description: "More than one pod has marketdata_leader=1. Check Redis connectivity."

      - alert: SubscriptionDrift
        expr: |
          abs(marketdata_subscriptions_active{application="market-data-service"}
              - on(application) marketdata_subscriptions_registry{application="market-data-service"})
              > 5
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Active EMA subscriptions diverge from registry"

      - alert: HighGapRate
        expr: rate(marketdata_kafka_gaps_published_total[5m]) > 0.1
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Frequent gap events — possible instability"

      - alert: KafkaPublishFailures
        expr: rate(marketdata_kafka_publish_failures_total[5m]) > 0
        for: 2m
        labels: { severity: warning }

      - alert: LsegConnectionDown
        expr: up{job="market-data-service"} == 1
             and on(pod) (
               kube_pod_status_ready{pod=~"market-data-service-(0|1)"}
             ) == 0
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "Warm-eligible pod's readiness is failing — LSEG conn likely down"
```

---

## Runbooks

### R1 — `NoLeader` firing

**Symptoms:** All `marketdata_leader` gauges = 0; subscribe/unsubscribe requests pile up in `market-data-requests` lag.

**Diagnose:**
1. Are warm-eligible pods even running? `oc get pods -l app=market-data-service`
2. Is Redis reachable? `oc exec market-data-service-0 -- redis-cli -h $REDIS_HOST ping`
3. Is the lock key permanently held by a dead pod?
   ```bash
   oc exec market-data-service-0 -- redis-cli -h $REDIS_HOST get marketdata:leader:lock
   ```

**Resolve:**
- If the lock is held by a non-existent pod (e.g. one that crashed without `PreDestroy`): wait for the 5s TTL. If it persists, manually delete:
  ```bash
  oc exec market-data-service-0 -- redis-cli -h $REDIS_HOST del marketdata:leader:lock
  ```
- If Redis is down, fix Redis first; the app will recover automatically.

### R2 — `MultipleLeaders` firing

**Symptoms:** Two or more pods think they're leader. Ticks may be double-published.

**Diagnose:**
- Is there a Redis network partition between the pods?
- Is the lock TTL too short for Redis latency? (Check `marketdata_redis_ping_latency`.)

**Resolve:**
1. Forcibly kill one of the leaders:
   ```bash
   oc delete pod <one-of-the-leaders> --force --grace-period=0
   ```
2. The remaining leader will continue. Investigate Redis stability before further action.

### R3 — Failover taking too long

**Target:** 5–7s. If routinely 10s+, check:
- JVM startup time on the warm-standby pod (`/actuator/startup`)
- Redis latency
- GC pauses around the failover event (check `jvm_gc_pause_seconds_max`)
- Number of RICs being resubscribed (large registries take longer — consider batching)

**Tune:** Lower `marketdata.leader.lock-ttl-seconds` to `3` and `heartbeat-seconds` to `1` — but watch for false-positives on GC pauses (check audit log for spurious `LEADER_LOST` events).

### R4 — Gap event spam

**Symptoms:** `marketdata_kafka_gaps_published_total` increases rapidly even when no failover is happening.

**Diagnose:**
- LSEG stream dropping intermittently — check `subscription_audit` for `LSEG_DISCONNECTED` events
- Check EMA reconnect config in `EmaConfig.xml` — `ReconnectAttemptLimit`, `ReconnectMinDelay`

**Resolve:**
- Increase `marketdata.recovery.gap-threshold-millis` from `2000` to `5000` to filter out short hiccups.
- If LSEG is genuinely unstable, escalate to LSEG support.

### R5 — Stuck subscription (refcount mismatch)

**Symptoms:** Business MS reports it's no longer subscribed, but `marketdata_subscriptions_active` shows the RIC still active.

**Diagnose:**
```bash
oc exec market-data-service-0 -- redis-cli -h $REDIS_HOST get marketdata:subscription:refcount:<RIC>
oc exec market-data-service-0 -- psql ... -c "SELECT * FROM subscription_requests WHERE ric='<RIC>';"
```

**Resolve:**
- If refcount in Redis disagrees with PG row count, PG is source of truth. Reset Redis:
  ```bash
  oc exec ... -- redis-cli del marketdata:subscription:refcount:<RIC>
  ```
- Then send a manual unsubscribe to force the drain:
  ```bash
  echo '<RIC>:{"ric":"<RIC>","action":"UNSUBSCRIBE","requester":"ops-manual"}' | kafka-producer ...
  ```

---

## Logging

JSON logs are emitted in OpenShift (Logback `openshift` profile). They include:
- `app`, `pod` — service + instance tags
- MDC values: `ric`, `requester`, `leaderRole`

Useful filters (Loki / Elastic):

```
# All events for a specific RIC
{app="market-data-service"} | json | ric="EUR="

# Failover events
{app="market-data-service"} |~ "State transition.*LEADER"

# Subscription churn for a business MS
{app="market-data-service"} | json | requester="trading-ms"
```

---

## Backup & recovery

- **PostgreSQL** — back up `ric_registry`, `subscription_requests`, `subscription_audit`, `market_data_gaps`. Daily logical backups (pg_dump) are sufficient.
- **Redis** — ephemeral cache; **no backup needed**. The app rebuilds Redis state on leader election from the PG registry.
- **Kafka topics** — retention is set per topic (24h–30d). For DR, rely on Kafka's own replication.

### Restoring after a total DB loss

1. Restore PG backup → schema + active RIC list is back.
2. Restart all pods → on leader election, the new leader will:
   - Read `ric_registry` from PG (active RICs)
   - Open EMA session
   - Send batch `ReqMsg` to LSEG
   - Snapshot ticks flow → cache rebuilds in Redis
   - First snapshot tick per RIC is published to `market-data-updates`
3. Business MSs will see brief gap events on `market-data-control` and can decide how to handle.

---

## Capacity planning

| Resource | Per pod | Comment |
|---|---|---|
| CPU | 500m request / 2000m limit | Tick processing is mostly I/O; CPU spikes on snapshot bursts |
| Memory | 1Gi request / 2Gi limit | Heap is ~75% of limit via `MaxRAMPercentage=75` |
| Network | Modest | ~10 KB/s per RIC depending on update rate |
| Storage | None | Stateless container |

PostgreSQL & Redis sizing depends on RIC count + audit retention:
- 1000 active RICs ≈ 50 KB in `ric_registry`, ~5 MB/day in `subscription_audit`
- Redis: 1000 RICs × (1KB price cache + counter) ≈ 1 MB

Kafka throughput at 1000 RICs × 10 ticks/s = ~10 K msg/s × ~500B = 5 MB/s on `market-data-updates`. Single broker handles this easily; size for retention.
