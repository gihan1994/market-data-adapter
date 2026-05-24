# OpenShift manifests — dual-cluster deployment

The service is deployed to **two OpenShift clusters** (hall1 + hall2) sharing a single
Enterprise Redis cluster for cross-hall leader election. Manifests are split into
`base/` (common) and `hall1/` / `hall2/` (per-hall overlays).

## Directory layout

```
deploy/openshift/
├── base/                         # identical across both halls
│   ├── statefulset.yaml          # 2 replicas per hall (= 4 pods total)
│   ├── service.yaml
│   ├── pdb.yaml
│   └── secret.template.yaml      # same secrets applied to both halls
├── hall1/
│   └── configmap.yaml            # HALL=hall1 + hall1 ADS endpoints + egress IP
└── hall2/
    └── configmap.yaml            # HALL=hall2 + hall2 ADS endpoints + egress IP
```

## Pod role matrix

| Pod | Hall | Role | Active DACS session | When elected leader |
|---|---|---|---|---|
| `market-data-service-0` | hall1 | `warm-eligible` | yes (1 session) | currently active subscriber |
| `market-data-service-1` | hall1 | `cold-only` | no | within-hall failover capacity |
| `market-data-service-0` | hall2 | `warm-eligible` | yes (1 session) | cross-hall warm standby |
| `market-data-service-1` | hall2 | `cold-only` | no | last-resort capacity |

**Total active DACS sessions: 2** — counted against the same per-user license.

## Apply procedure

```bash
# --- HALL 1 ---
oc --context hall1-cluster new-project marketdata
oc --context hall1-cluster apply -f hall1/configmap.yaml
oc --context hall1-cluster apply -f base/secret.template.yaml    # after filling in values
oc --context hall1-cluster apply -f base/statefulset.yaml
oc --context hall1-cluster apply -f base/service.yaml
oc --context hall1-cluster apply -f base/pdb.yaml

# --- HALL 2 (same files except per-hall configmap) ---
oc --context hall2-cluster new-project marketdata
oc --context hall2-cluster apply -f hall2/configmap.yaml
oc --context hall2-cluster apply -f base/secret.template.yaml    # SAME secrets as hall1
oc --context hall2-cluster apply -f base/statefulset.yaml
oc --context hall2-cluster apply -f base/service.yaml
oc --context hall2-cluster apply -f base/pdb.yaml
```

## Verification

```bash
# Check which hall holds the global leader lock
oc --context hall1-cluster exec market-data-service-0 -- \
  redis-cli -h $REDIS_HOST -a $REDIS_PASSWORD get marketdata:leader:lock
# Expected output: "hall1/market-data-service-0"  (or hall2/... after cross-hall failover)
```

```bash
# Check per-pod state
for hall in hall1 hall2; do
  for i in 0 1; do
    state=$(oc --context $hall-cluster exec market-data-service-$i -- \
            curl -s localhost:8080/actuator/health/leader | jq -r .details.state)
    echo "$hall/market-data-service-$i: $state"
  done
done
```

Expected: exactly one pod reports `LEADER`; one warm-eligible pod (the other hall) reports
`WARM_STANDBY`; cold-only pods report `COLD_STANDBY`.

## See also

- **[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)** — full architectural design
- **[docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md)** — step-by-step deployment guide
- **[docs/DACS_LICENSING.md](../../docs/DACS_LICENSING.md)** — licensing model explained
