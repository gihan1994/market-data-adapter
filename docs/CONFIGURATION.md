# Configuration

All configuration is via Spring profiles + environment variables.

## Profiles

| Profile | When | Source |
|---|---|---|
| `local` | Local dev (Docker Compose or laptop JVM) | `application-local.yml` |
| `openshift` | Deployed to OpenShift | `application-openshift.yml` |
| `test` | Integration tests with Testcontainers | `src/test/resources/application-test.yml` |

Set via `SPRING_PROFILES_ACTIVE` env var. Default: `local`.

---

## Environment variables

### Per-pod identity

| Env var | Required | Default | Notes |
|---|---|---|---|
| `POD_NAME` | yes | `local-pod` | OpenShift downward-API injects `metadata.name`. Used for leader-lock holder, audit trail, metrics tag. |
| `POD_ROLE` | yes | `warm-eligible` | `warm-eligible` or `cold-only`. Determines whether pod opens an EMA session at startup. |

### Database

| Env var | Required | Default | Notes |
|---|---|---|---|
| `DB_URL` | yes | `jdbc:postgresql://localhost:5432/marketdata` | JDBC URL |
| `DB_USER` | yes | `marketdata` | |
| `DB_PASSWORD` | yes | `marketdata` | Use a Secret in OpenShift |

### Redis

| Env var | Required | Default | Notes |
|---|---|---|---|
| `REDIS_HOST` | yes (if not cluster) | `localhost` | |
| `REDIS_PORT` | no | `6379` | |
| `REDIS_PASSWORD` | no | _(none)_ | Use a Secret in OpenShift |
| `REDIS_CLUSTER` | no | `false` | Set `true` for cluster mode |
| `REDIS_CLUSTER_NODES` | yes (if cluster) | _(none)_ | Comma-separated `host:port` list |

### Kafka

| Env var | Required | Default | Notes |
|---|---|---|---|
| `KAFKA_BOOTSTRAP` | yes | `localhost:9092` | Bootstrap server(s) |
| `KAFKA_SASL_JAAS` | OpenShift only | _(none)_ | JAAS config string for SASL/SCRAM auth |

### LSEG

| Env var | Required | Default | Notes |
|---|---|---|---|
| `LSEG_CLIENT_ID` | prod | _(empty)_ | RTO V2 OAuth client ID |
| `LSEG_CLIENT_SECRET` | prod | _(empty)_ | RTO V2 OAuth client secret. **Always use a Secret.** |
| `LSEG_TOKEN_URL` | no | `https://api.refinitiv.com/auth/oauth2/v2/token` | |
| `LSEG_MOCK` | local dev | `false` | `true` skips real EMA connection — useful for dev/test |

---

## Application properties

All under the `marketdata.*` prefix. Defined in `MarketDataProperties.java` with validation.

### `marketdata.pod`

| Property | Default | Description |
|---|---|---|
| `marketdata.pod.role` | `warm-eligible` | See `POD_ROLE` env var |
| `marketdata.pod.name` | `local-pod` | See `POD_NAME` env var |

### `marketdata.leader`

| Property | Default | Description |
|---|---|---|
| `marketdata.leader.lock-key` | `marketdata:leader:lock` | Redis key for the distributed lock |
| `marketdata.leader.lock-ttl-seconds` | `5` | Lock TTL. Lower = faster failover, higher risk of false-positive on GC pause |
| `marketdata.leader.heartbeat-seconds` | `2` | How often the leader renews the lock |
| `marketdata.leader.acquire-retry-seconds` | `1` | (unused currently — heartbeat doubles as retry) |

### `marketdata.lseg`

| Property | Default | Description |
|---|---|---|
| `marketdata.lseg.client-id` | _empty_ | See `LSEG_CLIENT_ID` |
| `marketdata.lseg.client-secret` | _empty_ | See `LSEG_CLIENT_SECRET` |
| `marketdata.lseg.token-url` | `https://api.refinitiv.com/auth/oauth2/v2/token` | OAuth2 token endpoint |
| `marketdata.lseg.scope` | `trapi` | OAuth2 scope |
| `marketdata.lseg.ema-config-file` | `classpath:EmaConfig.xml` | Path to EMA config XML |
| `marketdata.lseg.consumer-name` | `Consumer_1` | Must match a `<Consumer>` in `EmaConfig.xml` |
| `marketdata.lseg.service-name` | `ELEKTRON_DD` | LSEG service name |
| `marketdata.lseg.mock-mode` | `false` | See `LSEG_MOCK` |

### `marketdata.subscription`

| Property | Default | Description |
|---|---|---|
| `marketdata.subscription.drain-grace-seconds` | `30` | How long to wait after refcount→0 before closing the EMA stream |
| `marketdata.subscription.batch-resubscribe-size` | `100` | (reserved — batch size for resubscription during failover) |
| `marketdata.subscription.refcount-key-prefix` | `marketdata:subscription:refcount:` | Redis key prefix |
| `marketdata.subscription.active-rics-key` | `marketdata:ric:active` | Redis sorted-set key |
| `marketdata.subscription.price-key-prefix` | `marketdata:ric:price:` | Redis key prefix for latest-price cache |
| `marketdata.subscription.last-published-key-prefix` | `marketdata:ric:last-published:` | Redis key prefix for gap-tracking timestamps |
| `marketdata.subscription.price-ttl-seconds` | `60` | TTL on cached prices |

### `marketdata.kafka`

| Property | Default | Description |
|---|---|---|
| `marketdata.kafka.topic-requests` | `market-data-requests` | Subscribe / unsubscribe requests in |
| `marketdata.kafka.topic-updates` | `market-data-updates` | Live ticks out |
| `marketdata.kafka.topic-control` | `market-data-control` | Gap + recovery events out |

### `marketdata.recovery`

| Property | Default | Description |
|---|---|---|
| `marketdata.recovery.gap-threshold-millis` | `2000` | Below this, the gap is ignored (transient hiccup, not a failover) |
| `marketdata.recovery.snapshot-on-recovery` | `true` | (reserved — emit a snapshot tick alongside GAP_DETECTED) |

---

## LSEG credentials

For production, obtain LSEG **RTO V2 OAuth2 client credentials** from your account manager. They look like:

```
client_id     = GE-A-12345678-9-0123
client_secret = <opaque string>
```

Store them as a Kubernetes Secret:

```bash
oc create secret generic market-data-service-lseg \
  --from-literal=client-id='GE-A-12345678-9-0123' \
  --from-literal=client-secret='<opaque string>'
```

The StatefulSet manifest already wires these into the `LSEG_CLIENT_ID` / `LSEG_CLIENT_SECRET` env vars.

For local development, **set `LSEG_MOCK=true`** to skip the real connection.

---

## EMA configuration

The EMA SDK is configured via `src/main/resources/EmaConfig.xml`. Defaults:

- `Consumer_1` uses `Channel_RTO` (cloud / RTO)
- `Channel_RTO` has `EncryptedProtocolType=RSSL_SOCKET` + `EnableSessionManagement=1` for automatic OAuth2 + service discovery
- Region: `us-east-1` — change `<Location>` to `eu-west-1`, `ap-northeast-1`, etc. as appropriate
- Reconnection: unlimited attempts, 2s min delay, 30s max delay

For on-prem ADS deployments, switch the consumer's `Channel` to `Channel_RSSL` and set the `Host`/`Port` accordingly.

---

## Per-pod role assignment (OpenShift)

The StatefulSet template uses a `ConfigMap`-as-key-lookup pattern:

```yaml
env:
  - name: POD_NAME
    valueFrom: { fieldRef: { fieldPath: metadata.name } }
  - name: POD_ROLE
    valueFrom: { configMapKeyRef: { name: market-data-service-pod-roles, key: $(POD_NAME) } }
```

The ConfigMap (`deploy/openshift/configmap.yaml`) maps pod names to roles:

```yaml
data:
  market-data-service-0: "warm-eligible"
  market-data-service-1: "warm-eligible"
  market-data-service-2: "cold-only"
  market-data-service-3: "cold-only"
```

To change the warm/cold split, edit the ConfigMap and `oc rollout restart statefulset/market-data-service`.
