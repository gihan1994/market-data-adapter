# Dual-Cluster OpenShift Deployment

## Prerequisites

| Item | Detail |
|---|---|
| OpenShift clusters | 2 — one in hall1, one in hall2 (versions 4.10+) |
| `oc` contexts | Configured for both clusters (e.g. `hall1-cluster` and `hall2-cluster`) |
| Container registry | Reachable from both clusters (internal registry, Quay, or ECR) |
| **Enterprise Redis** | Active-active across both halls (Redis Enterprise CRDB or master-replica) |
| PostgreSQL | Cross-hall HA (Crunchy Postgres synchronous standby, or AWS RDS Multi-AZ) |
| Kafka | Multi-DC (Strimzi MirrorMaker, Confluent Replicator, or stretched cluster) |
| LSEG TREP | Single load balancer (HA pair) reachable from BOTH halls. Hall1 and hall2 egress IPs whitelisted at DACS. |

## File layout

```
deploy/openshift/
├── base/                          # identical across both halls
│   ├── statefulset.yaml           # 2 replicas per hall (= 4 pods total)
│   ├── service.yaml
│   ├── pdb.yaml
│   └── secret.template.yaml       # SAME secrets applied to both halls
├── hall1/
│   └── configmap.yaml             # HALL=hall1 + hall1 ADS + 10.10.1.100 egress IP
└── hall2/
    └── configmap.yaml             # HALL=hall2 + hall2 ADS + 10.10.2.100 egress IP
```

---

## Step-by-step

### 1. Build & push the image

```bash
docker build -t market-data-adapter:1.0.0 .
docker tag market-data-adapter:1.0.0 <registry>/marketdata/market-data-adapter:1.0.0
docker push <registry>/marketdata/market-data-adapter:1.0.0
```

Both clusters must be able to pull this image. If using OpenShift's internal registry per cluster, push to both.

### 2. Coordinate with TREP team

Before any deployment:
- [ ] **DACS user created** (e.g., `marketdata-adapter`)
- [ ] **MaxLogins ≥ 3** on that user
- [ ] **Both hall egress IPs whitelisted** (`10.10.1.100`, `10.10.2.100`)
- [ ] **Application ID 256** confirmed (or get one assigned)
- [ ] **Product entitlements** added to the user

Capture the DACS password — you'll need it for the Secret in step 4.

### 3. Prepare ConfigMaps per hall

Edit `deploy/openshift/hall1/configmap.yaml` and `hall2/configmap.yaml` with the **real** values:

- `lseg.ads-host` / `ads-backup-host` for each hall
- `lseg.dacs-position` (the actual whitelisted egress IPs)
- `kafka.bootstrap` per hall
- `redis.cluster-nodes` per hall (same logical cluster, different access endpoints)

### 4. Prepare and apply Secrets (same in both halls)

```bash
cp deploy/openshift/base/secret.template.yaml /tmp/secret.yaml
# Edit: fill REPLACE-ME values:
#   - dacs-username: marketdata-adapter
#   - db / kafka / redis passwords
oc --context hall1-cluster apply -f /tmp/secret.yaml
oc --context hall2-cluster apply -f /tmp/secret.yaml
rm /tmp/secret.yaml
```

For production, use SealedSecrets or ExternalSecrets — never commit real values.

### 5. Deploy to hall1

```bash
oc --context hall1-cluster new-project marketdata
oc --context hall1-cluster apply -f deploy/openshift/hall1/configmap.yaml
oc --context hall1-cluster apply -f deploy/openshift/base/statefulset.yaml
oc --context hall1-cluster apply -f deploy/openshift/base/service.yaml
oc --context hall1-cluster apply -f deploy/openshift/base/pdb.yaml
oc --context hall1-cluster rollout status statefulset/market-data-service
```

### 6. Deploy to hall2

```bash
oc --context hall2-cluster new-project marketdata
oc --context hall2-cluster apply -f deploy/openshift/hall2/configmap.yaml
oc --context hall2-cluster apply -f deploy/openshift/base/statefulset.yaml
oc --context hall2-cluster apply -f deploy/openshift/base/service.yaml
oc --context hall2-cluster apply -f deploy/openshift/base/pdb.yaml
oc --context hall2-cluster rollout status statefulset/market-data-service
```

### 7. Watch leader election converge

```bash
# Check which hall holds the global lock
oc --context hall1-cluster exec market-data-service-0 -- \
  redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD get marketdata:leader:lock
# Expected: "hall1/market-data-service-0" or "hall2/market-data-service-0"
```

---

## Verification matrix

```bash
for hall in hall1 hall2; do
  for i in 0 1; do
    role=$(oc --context $hall-cluster exec market-data-service-$i -- \
           curl -s localhost:8080/actuator/health/leader 2>/dev/null \
           | grep -oE '"state":"[^"]+"' | cut -d'"' -f4)
    echo "$hall/market-data-service-$i: $role"
  done
done
```

Expected steady state:

| Pod | Expected state |
|---|---|
| `hall1/market-data-service-0` | `LEADER` OR `WARM_STANDBY` |
| `hall1/market-data-service-1` | `COLD_STANDBY` |
| `hall2/market-data-service-0` | `WARM_STANDBY` OR `LEADER` |
| `hall2/market-data-service-1` | `COLD_STANDBY` |

Exactly **one** pod should be `LEADER`; exactly **one** warm-eligible pod should be `WARM_STANDBY`.

```bash
# Check DACS sessions from EMA side
oc --context hall1-cluster exec market-data-service-0 -- \
  curl -s localhost:8080/actuator/prometheus | grep marketdata_subscriptions_active
oc --context hall2-cluster exec market-data-service-0 -- \
  curl -s localhost:8080/actuator/prometheus | grep marketdata_subscriptions_active

# Expected: leader pod shows N>0 (= number of subscribed RICs)
#           warm pod shows 0 (open session, no subscriptions)
```

---

## Failover testing

### Within-hall pod failover

```bash
# Identify the leader
LEADER_POD=$(...)  # pod that has marketdata_leader=1
LEADER_HALL=hall1  # or hall2

# Kill it
oc --context ${LEADER_HALL}-cluster delete pod $LEADER_POD

# Within ~5–7s, another pod takes over. If the killed pod was in hall1 and the cross-hall
# warm standby is in hall2, the lock goes to hall2/pod-0 (cross-hall promotion).
# If the killed pod was warm-eligible, hall2/pod-0 (existing warm standby) becomes leader.

oc --context hall2-cluster logs market-data-service-0 | grep -E "Promoted to leader"
```

### Cross-hall failover (full hall1 outage)

This is the failure mode that justifies the 2-session investment. Simulate by scaling hall1 to zero:

```bash
oc --context hall1-cluster scale statefulset/market-data-service --replicas=0
sleep 8
oc --context hall2-cluster logs market-data-service-0 | grep -E "Promoted to leader|GAP_DETECTED"
```

Expected sequence in `hall2/market-data-service-0`'s logs:

```
State transition: WARM_STANDBY → LEADER (lock acquired)
Promoted to leader — resubscribing all active RICs
Published GAP_DETECTED ric=...  duration=5000ms+
```

Recover hall1:
```bash
oc --context hall1-cluster scale statefulset/market-data-service --replicas=2
# hall1 pods come back as WARM_STANDBY / COLD_STANDBY — hall2 remains leader (no automatic fail-back)
```

### TREP LB endpoint failover (no application restart)

The TREP LB exposes two IPs (`trep-lb-1`, `trep-lb-2`). If the LB drops one IP (because a backend ADS failed and the LB collapsed to a single backend), EMA `ChannelSet` reconnects to the remaining endpoint transparently — no new DACS session opens. To verify: ask your TREP admin to fail one of the LB endpoints and watch:
- `marketdata_kafka_publish_failures_total` should stay at 0
- Active subscription count gauge should not drop
- A short tick gap (~2–5s) may appear

---

## Scaling

### Add cold-standby capacity (per hall)

```bash
oc --context hall1-cluster scale statefulset/market-data-service --replicas=3
```

Then update the hall ConfigMap:
```yaml
market-data-service-2: "cold-only"
```

### Promote a cold pod to warm

⚠ **Cost increase** — this opens an additional DACS session.

Edit the ConfigMap:
```yaml
market-data-service-1: "warm-eligible"   # was "cold-only"
```

```bash
oc --context hall1-cluster apply -f deploy/openshift/hall1/configmap.yaml
oc --context hall1-cluster rollout restart statefulset/market-data-service
```

Confirm `MaxLogins` on your DACS user can accommodate the increase.

---

## Routing inbound HTTP traffic

The Service is `ClusterIP` per hall. For external access add a Route per hall, or front with a global load-balancer that detects which hall holds the leader (use the `marketdata_leader` metric).

> The HTTP endpoints (`/actuator/*`) are stateless — any pod can serve them. Business logic only happens on the leader. Most installations don't need to route based on leader; they query metrics directly.

---

## Common deployment issues

### `marketdata_leader = 0` on all pods

**Cause:** Redis lock unreachable, or all warm-eligible pods are unhealthy.

```bash
# Check Redis connectivity from each cluster
oc --context hall1-cluster exec market-data-service-0 -- nc -zv $REDIS_HOST 6379
oc --context hall2-cluster exec market-data-service-0 -- nc -zv $REDIS_HOST 6379

# Check the lock state
oc --context hall1-cluster exec market-data-service-0 -- \
  redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD get marketdata:leader:lock
```

### Two leaders simultaneously (split-brain)

**Cause:** Network partition between halls + Redis active-active write conflict.

- For Redis Enterprise CRDB: CRDT resolution should converge within seconds. Check for stuck conflict counters.
- For Redis master-replica: a partition can promote both sides. Ensure quorum is required for writes (sentinel + odd number of sentinels).

```bash
# Force a single leader
oc --context <minority-side>-cluster scale statefulset/market-data-service --replicas=0
```

### DACS rejects login: "MaxLogins exceeded"

**Cause:** Your user has too many residual sessions on ADS.

```bash
# Ask TREP admin
adsadmin> show user marketdata-adapter
adsadmin> kill session marketdata-adapter 10.10.1.100   # if a session is stuck
```

Increase `MaxLogins` to ≥ 3 to avoid recurrence.

### `Connection refused` to ADS from one hall only

**Cause:** Egress IP for that hall is not yet whitelisted at TREP.

```bash
# Check the egress IP the pod is using
oc --context hall1-cluster exec market-data-service-0 -- curl -s ifconfig.io
# Should match LSEG_DACS_POSITION
```

Send the actual observed egress IP to the TREP team for whitelisting.

---

## Rollback

```bash
oc --context hall1-cluster rollout undo statefulset/market-data-service
oc --context hall2-cluster rollout undo statefulset/market-data-service
```

> Do NOT rollback the DB schema — Flyway migrations are forward-only. Write a new migration if needed.
