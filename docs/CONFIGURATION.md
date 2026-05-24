# Configuration

All configuration is via Spring profiles + environment variables.

## Profiles

| Profile | When | Source |
|---|---|---|
| `local` | Local dev (Docker Compose or laptop JVM) | `application-local.yml` |
| `openshift` | Deployed to OpenShift (hall1 or hall2) | `application-openshift.yml` |
| `test` | Integration tests with Testcontainers | `src/test/resources/application-test.yml` |

Set via `SPRING_PROFILES_ACTIVE`. Default: `local`.

---

## Environment variables

### Per-pod identity (cluster + role)

| Env var | Required | Default | Notes |
|---|---|---|---|
| `HALL` | yes | `hall1` | Cluster identifier — `hall1` or `hall2`. Set via per-hall ConfigMap (`market-data-service-config.hall`). |
| `POD_ROLE` | yes | `warm-eligible` | `warm-eligible` or `cold-only`. Determines whether the pod opens an EMA session at startup. |
| `POD_NAME` | yes | `local-pod` | OpenShift downwardAPI `metadata.name`. Used in leader-lock holder ID, audit log, metrics. |

### Database

| Env var | Required | Default | Notes |
|---|---|---|---|
| `DB_URL` | yes | `jdbc:postgresql://localhost:5432/marketdata` | JDBC URL — cross-hall HA DB |
| `DB_USER` | yes | `marketdata` | |
| `DB_PASSWORD` | yes | _(none)_ | OpenShift Secret |

### Redis (Enterprise)

| Env var | Required | Default | Notes |
|---|---|---|---|
| `REDIS_HOST` | local dev | `localhost` | Single-node mode |
| `REDIS_PORT` | no | `6379` | |
| `REDIS_PASSWORD` | no | _(none)_ | OpenShift Secret |
| `REDIS_CLUSTER` | OpenShift | `false` | Set `true` for Enterprise Redis cluster |
| `REDIS_CLUSTER_NODES` | OpenShift | _(none)_ | Comma-separated `host:port`. The cluster spans both halls. |

### Kafka

| Env var | Required | Default | Notes |
|---|---|---|---|
| `KAFKA_BOOTSTRAP` | yes | `localhost:9092` | Multi-DC Kafka bootstrap |
| `KAFKA_SASL_JAAS` | OpenShift | _(none)_ | JAAS config for SASL/SCRAM |

### LSEG connection mode

| Env var | Required | Default | Notes |
|---|---|---|---|
| `LSEG_CONNECTION_MODE` | yes | `ON_PREM_TREP` | `ON_PREM_TREP` or `RTO_CLOUD` |
| `LSEG_MOCK` | local dev | `false` | `true` skips real EMA connection for dev/test |

### LSEG — on-prem TREP (DACS auth) ⭐

Used when `LSEG_CONNECTION_MODE=ON_PREM_TREP` (production default).

| Env var | Required | Default | Notes |
|---|---|---|---|
| `LSEG_DACS_USERNAME` | yes | _(empty)_ | DACS named user. **Same value in both halls.** OpenShift Secret. |
| `LSEG_DACS_POSITION` | yes | _(empty)_ | Static egress IP of THIS hall's namespace. **Different per hall.** Per-hall ConfigMap. |
| `LSEG_DACS_APPLICATION_ID` | yes | `256` | LSEG application ID. Same in both halls. |
| `LSEG_ADS_HOST` | yes | _(empty)_ | Primary ADS endpoint for THIS hall. Per-hall ConfigMap. |
| `LSEG_ADS_PORT` | no | `14002` | |
| `LSEG_ADS_BACKUP_HOST` | yes | _(empty)_ | Backup ADS in SAME hall (for ChannelSet failover). |
| `LSEG_ADS_BACKUP_PORT` | no | `14002` | |
| `LSEG_SERVICE_NAME` | yes | `ELEKTRON_DD` | ADS service name |

### LSEG — RTO cloud (OAuth2)

Used when `LSEG_CONNECTION_MODE=RTO_CLOUD`. Optional path — kept for hybrid deployments.

| Env var | Required | Default | Notes |
|---|---|---|---|
| `LSEG_CLIENT_ID` | RTO only | _(empty)_ | OAuth2 V2 client ID |
| `LSEG_CLIENT_SECRET` | RTO only | _(empty)_ | OpenShift Secret |
| `LSEG_TOKEN_URL` | no | `https://api.refinitiv.com/auth/oauth2/v2/token` | |

---

## `marketdata.*` application properties

Defined in `MarketDataProperties.java` with `@Validated` constraints. The properties tree:

```yaml
marketdata:
  pod:
    hall: hall1                  # ${HALL}
    role: warm-eligible          # ${POD_ROLE}
    name: market-data-service-0  # ${POD_NAME}
  leader:
    lock-key: marketdata:leader:lock
    lock-ttl-seconds: 5
    heartbeat-seconds: 2
    acquire-retry-seconds: 1
  lseg:
    connection-mode: ON_PREM_TREP
    dacs-username: ${LSEG_DACS_USERNAME}
    dacs-position: ${LSEG_DACS_POSITION}
    dacs-application-id: ${LSEG_DACS_APPLICATION_ID}
    ads-host: ${LSEG_ADS_HOST}
    ads-port: ${LSEG_ADS_PORT}
    ads-backup-host: ${LSEG_ADS_BACKUP_HOST}
    ads-backup-port: ${LSEG_ADS_BACKUP_PORT}
    service-name: ELEKTRON_DD
    consumer-name: Consumer_1
    mock-mode: false
  subscription:
    drain-grace-seconds: 30
    refcount-key-prefix: "marketdata:subscription:refcount:"
    active-rics-key: "marketdata:ric:active"
    price-key-prefix: "marketdata:ric:price:"
    last-published-key-prefix: "marketdata:ric:last-published:"
    price-ttl-seconds: 60
  kafka:
    topic-requests: market-data-requests
    topic-updates: market-data-updates
    topic-control: market-data-control
  recovery:
    gap-threshold-millis: 2000
    snapshot-on-recovery: true
```

---

## Per-hall configuration

Each OpenShift cluster gets its own `market-data-service-config` ConfigMap. The differences:

| Key | hall1 ConfigMap | hall2 ConfigMap |
|---|---|---|
| `hall` | `hall1` | `hall2` |
| `kafka.bootstrap` | `kafka-...hall1.svc:9092,...` | `kafka-...hall2.svc:9092,...` |
| `redis.cluster-nodes` | `redis-...hall1.svc:10000,...` | `redis-...hall2.svc:10000,...` (same cluster, different access points) |
| `lseg.ads-host` | `trep-lb-1.trep.local` ⭐ same in both halls | `trep-lb-1.trep.local` |
| `lseg.ads-backup-host` | `trep-lb-2.trep.local` ⭐ same in both halls | `trep-lb-2.trep.local` |
| `lseg.dacs-position` | `10.10.1.100` (hall1 egress IP) | `10.10.2.100` (hall2 egress IP) |

Identical across halls:
- Secrets (`market-data-service-db`, `-kafka`, `-redis`, `-lseg`)
- `lseg.service-name`, `lseg.dacs-application-id`
- `lseg.dacs-username` (same DACS user → same secret in both)
- Pod role assignments (each hall has pod-0 = warm-eligible, pod-1 = cold-only)

See `deploy/openshift/hall1/configmap.yaml` and `deploy/openshift/hall2/configmap.yaml` for the actual values.

---

## EMA configuration (`EmaConfig.xml`)

Located at `src/main/resources/EmaConfig.xml`. Key items:

- **Consumer_1** uses `ChannelSet="Channel_TREP_LB_Primary, Channel_TREP_LB_Backup"` for transparent failover between the two TREP LB endpoints (no new DACS login on switchover).
- Channel host/port values in the XML are **placeholders** — the actual endpoint is set via `OmmConsumerConfig.host()` from `LSEG_ADS_HOST` env var at runtime.
- `Channel_RTO` is defined but only used when `connection-mode=RTO_CLOUD`. Switch the consumer's `Channel` from `ChannelSet` to `Channel_RTO` if migrating to cloud.
- `ReconnectAttemptLimit=-1` (unlimited), `ReconnectMinDelay=2000ms`, `ReconnectMaxDelay=30000ms`.
- `Dictionary_1` = `ChannelDictionary` (downloads from ADS). For air-gapped deployments, switch to `FileDictionary` + local `RDMFieldDictionary` and `enumtype.def`.

---

## LSEG credentials

For production, obtain from your TREP admin team:

1. **DACS named user** + password (we suggest `marketdata-adapter`)
2. **Egress IP whitelisting** — both hall egress IPs (`10.10.1.100` and `10.10.2.100`) added to the user's allowed-positions list
3. **MaxLogins ≥ 3** on that user (allows steady state of 2 + brief overlap during failover)
4. **Application ID** — confirm `256` works, or get one assigned

Store in OpenShift Secret:

```bash
oc create secret generic market-data-service-lseg \
  --from-literal=dacs-username='marketdata-adapter' \
  --from-literal=dacs-password='<from-trep-team>'
```

> Note: the LOGIN domain in EMA uses username + position + applicationId. Password is configured via `OmmConsumerConfig.password()` if your DACS requires it — most installations use position-based auth only. See `OmmConsumerManager.configureForOnPremTrep()`.

For local development, **set `LSEG_MOCK=true`** to skip the real connection. See [DACS_LICENSING.md](DACS_LICENSING.md) for the full licensing model.
