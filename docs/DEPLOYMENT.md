# OpenShift Deployment

## Prerequisites

- OpenShift cluster (4.10+) with `oc` CLI
- A namespace (`oc new-project marketdata`)
- Image registry access (internal OpenShift registry or external)
- External infrastructure available:
  - PostgreSQL (e.g. CloudNativePG, Crunchy Postgres)
  - Redis (with cluster mode if HA required)
  - Kafka (e.g. Strimzi, Confluent Operator)
- LSEG RTO V2 OAuth credentials

## File layout

```
deploy/openshift/
├── statefulset.yaml          # 4-replica StatefulSet + headless service
├── service.yaml              # ClusterIP service for HTTP/actuator
├── configmap.yaml            # Kafka/Redis endpoints + pod-role map
├── secret.template.yaml      # Template for DB/Kafka/Redis/LSEG secrets
├── pdb.yaml                  # PodDisruptionBudget — at most 1 pod down
└── README.md                 # Operational notes
```

---

## Step-by-step deployment

### 1. Build & push the image

```bash
# Tag for the internal registry
docker tag market-data-service:dev \
  image-registry.openshift-image-registry.svc:5000/marketdata/market-data-service:1.0.0

# OR push to your external registry (Quay, ECR, etc.)
docker tag market-data-service:dev quay.io/yourorg/market-data-service:1.0.0
docker push quay.io/yourorg/market-data-service:1.0.0
```

If using the internal registry, you can also use BuildConfig:

```bash
oc new-build --binary --strategy=docker --name=market-data-service
oc start-build market-data-service --from-dir=. --follow
```

### 2. Create the namespace and apply ConfigMaps

```bash
oc new-project marketdata
oc apply -f deploy/openshift/configmap.yaml
```

Edit `configmap.yaml` first to point at your real infrastructure endpoints.

### 3. Create Secrets

Copy the template and fill in real values:

```bash
cp deploy/openshift/secret.template.yaml /tmp/secret.yaml
# Edit /tmp/secret.yaml — replace all REPLACE-ME values
oc apply -f /tmp/secret.yaml
rm /tmp/secret.yaml   # don't leave credentials on disk
```

Or use individual `oc create secret` commands (safer for CI/CD):

```bash
oc create secret generic market-data-service-db \
  --from-literal=url='jdbc:postgresql://postgres-primary.db.svc:5432/marketdata' \
  --from-literal=user='marketdata' \
  --from-literal=password=$(read -rs PW; echo "$PW")

oc create secret generic market-data-service-lseg \
  --from-literal=client-id='YOUR-CLIENT-ID' \
  --from-literal=client-secret=$(read -rs SEC; echo "$SEC")

oc create secret generic market-data-service-redis \
  --from-literal=password=$(read -rs PW; echo "$PW")

oc create secret generic market-data-service-kafka \
  --from-literal=sasl-jaas='org.apache.kafka.common.security.scram.ScramLoginModule required username="marketdata" password="...";'
```

Better — use **SealedSecrets** or **External Secrets Operator** in production so credentials are version-controlled encrypted.

### 4. Deploy the workload

```bash
oc apply -f deploy/openshift/statefulset.yaml
oc apply -f deploy/openshift/service.yaml
oc apply -f deploy/openshift/pdb.yaml
```

### 5. Watch the rollout

```bash
oc rollout status statefulset/market-data-service
# or:
oc get pods -l app=market-data-service -w
```

You should see all 4 pods come up in parallel (the StatefulSet has `podManagementPolicy: Parallel`).

---

## Verifying the deployment

### Check leader election

```bash
for i in 0 1 2 3; do
  echo "=== market-data-service-$i ==="
  oc exec market-data-service-$i -- curl -s localhost:8080/actuator/health/leader
done
```

Expected:

| Pod | `state` | `isLeader` |
|---|---|---|
| `market-data-service-0` | `LEADER` *or* `WARM_STANDBY` | true *or* false |
| `market-data-service-1` | `LEADER` *or* `WARM_STANDBY` | true *or* false |
| `market-data-service-2` | `COLD_STANDBY` | false |
| `market-data-service-3` | `COLD_STANDBY` | false |

Exactly one of pod-0 and pod-1 should be `LEADER`.

### Check LSEG connection

```bash
oc exec market-data-service-0 -- curl -s localhost:8080/actuator/health/lseg
```

Expected: `{"status":"UP","details":{"emaConnected":true,"podRole":"WARM_ELIGIBLE",...}}`

### Stream metrics

```bash
oc exec market-data-service-0 -- curl -s localhost:8080/actuator/prometheus \
  | grep marketdata_
```

---

## Failover test

```bash
# Identify the current leader
LEADER=$(for i in 0 1; do
  STATE=$(oc exec market-data-service-$i -- curl -s localhost:8080/actuator/health/leader \
          | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
  if [ "$STATE" = "LEADER" ]; then echo "market-data-service-$i"; fi
done)
echo "Current leader: $LEADER"

# Kill it
oc delete pod $LEADER

# Watch the other warm-eligible pod take over
oc logs -f -l app=market-data-service --tail=20 | grep -E "(Promoted to leader|GAP_DETECTED)"
```

You should see:
- Within ~5s: the other warm-eligible pod logs `State transition: WARM_STANDBY → LEADER`
- Within ~7s: `Promoted to leader — resubscribing all active RICs`
- Within ~7s: `GAP_DETECTED` events published to `market-data-control`

The killed pod will be restarted by the StatefulSet and come up as the new `WARM_STANDBY`.

---

## Scaling

### Add cold-standby capacity

Edit the StatefulSet:
```bash
oc scale statefulset/market-data-service --replicas=6
```

Then update the ConfigMap to assign roles for the new ordinals:
```yaml
data:
  ...
  market-data-service-4: "cold-only"
  market-data-service-5: "cold-only"
```

```bash
oc apply -f deploy/openshift/configmap.yaml
oc rollout restart statefulset/market-data-service
```

### Promote a cold pod to warm

Change the ConfigMap entry from `cold-only` to `warm-eligible`. The pod will open an EMA session on its next restart → **adds 1 LSEG login charge**.

> ⚠ Don't have 3+ warm-eligible pods in the same cluster — only one can be leader at a time, and the others are wasted login fees.

---

## Routing inbound traffic

The `Service` is `ClusterIP` by default — for external access add a Route:

```yaml
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: market-data-service
spec:
  to: { kind: Service, name: market-data-service }
  port: { targetPort: http }
  tls: { termination: edge }
```

> Note: the service is **stateless from the HTTP perspective** — any pod can serve `/actuator/health`, etc. — but business logic only happens on the leader. There's no need for client-side routing.

---

## Common deployment issues

### Pods in `CrashLoopBackOff`

```bash
oc logs market-data-service-0 --previous --tail=60
```

Likely causes:
- Wrong DB credentials in Secret — check `oc describe secret market-data-service-db`
- Postgres not reachable from the pod — `oc rsh market-data-service-0` then `nc -zv postgres-primary.db.svc 5432`
- Flyway migration failure — usually means the schema already exists from a previous broken deploy. Run `oc exec market-data-service-0 -- flyway repair` (or delete + recreate the DB)

### All 4 pods think they're LEADER

The Redis connection is broken or the lock key is being flushed. Check:
- `oc exec market-data-service-0 -- redis-cli -h $REDIS_HOST get marketdata:leader:lock`
- Redis logs for evictions / restarts

### `marketdata_subscriptions_active` is high but `_registry` is 0

Refcounts in Redis are out of sync with PG. Usually means Redis was flushed/restarted. The leader will reconcile on next failover, but you can force it:

```bash
oc exec $LEADER -- redis-cli -h $REDIS_HOST DEL marketdata:ric:active
oc delete pod $LEADER   # forces failover + resubscribe from PG
```

### Failover taking longer than 7s

Increase `marketdata.leader.heartbeat-seconds` for stability, or check:
- Redis latency (`oc exec $POD -- redis-cli --latency -h $REDIS_HOST`)
- JVM GC pauses (`marketdata_jvm_gc_pause_seconds`)
- Network latency between pods and Redis

---

## Rollback

```bash
oc rollout undo statefulset/market-data-service
# or to a specific revision:
oc rollout history statefulset/market-data-service
oc rollout undo statefulset/market-data-service --to-revision=3
```

> The DB schema is migrated forward by Flyway. **Rollback the app but NOT the schema** — Flyway-managed migrations are not auto-reversed. If a schema change is incompatible, write a forward migration.
