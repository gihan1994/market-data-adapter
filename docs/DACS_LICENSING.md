# DACS Licensing Model

This document explains how LSEG TREP/RTDS bills our service and why the chosen architecture (2 active DACS sessions, same DACS user) is optimal.

## TL;DR

- **Licensing dimension:** named DACS user (we have **1 user**)
- **Technical constraint:** that user's `MaxLogins` setting (we use **2 concurrent sessions**)
- **Cost impact of more pods:** zero — each additional pod adds capacity, not licensing cost
- **Cost impact of opening more EMA sessions per pod:** zero billing-wise, but consumes ADS resources

## How TREP counts logins

When a pod calls `OmmConsumer.initialize()`, EMA opens a TCP connection to ADS and sends a `LOGIN` domain request with three identifiers:

| Field | What we set | Where |
|---|---|---|
| `username` | `marketdata-adapter` (a DACS named user) | `LSEG_DACS_USERNAME` secret |
| `position` | static hall egress IP (e.g. `10.10.1.100`) | `LSEG_DACS_POSITION` ConfigMap entry |
| `applicationId` | `256` (a numeric appId from the LSEG appId list) | `LSEG_DACS_APPLICATION_ID` ConfigMap entry |

The DACS server then:
1. Authenticates the user
2. Checks the position IP is allowed for that user (we asked the TREP team to whitelist both hall egress IPs)
3. Checks the user is below their `MaxLogins` ceiling
4. Permits subscriptions according to the user's product entitlements

**Each successful login counts as one "session".** Sharing a source IP does **not** deduplicate logins — that was a misconception we corrected during design review.

## Cost vs. session math

| Configuration | DACS users | Active concurrent sessions | Cost |
|---|---|---|---|
| 4 pods × 1 OmmConsumer each (naive) | 1 (shared) | 4 | 1 user fee |
| Our design (1 active + 1 warm + 2 cold) | 1 (shared) | **2** | 1 user fee |
| 4 fully active pods, different DACS users | 4 | 4 | 4 user fees ❌ |
| 1 leader only, others cold | 1 | 1 | 1 user fee + slow failover |

**The licensing model is per-user, so the cost is the same in the first three scenarios.** What matters is:
- **MaxLogins constraint** — DACS will reject the (N+1)-th session for the same user. Default is usually 5–10. Check with your TREP admin.
- **ADS resource use** — every session keeps a TCP connection + LOGIN context on the ADS. Best practice: minimize concurrent sessions.
- **Duplicate data delivery** — if both active sessions subscribe to the same RIC, ADS delivers the data twice (= 2× outbound bandwidth from ADS).

Our design avoids the last problem because only the **leader** subscribes; the warm session has 0 subscriptions, so it consumes near-zero ADS bandwidth.

## Why we chose "1 warm per hall" instead of "2 warm in hall1"

Both options cost the same (2 sessions). But:

| | 2 warm in hall1 | 1 warm in hall1, 1 warm in hall2 (current) |
|---|---|---|
| Within-hall pod failure | ~5s (warm session already open) | ~5s (warm session already open) |
| Full hall1 outage | **Total outage** — no warm session in hall2 | ~5–7s (hall2 warm pod takes over) |
| Network partition between halls | OK | OK (lock holder remains leader) |
| Risk of split-brain | Low | Slightly elevated — Enterprise Redis active-active mitigates |

Cross-hall warm wins because it tolerates DC-level failure for the same cost.

## DACS configuration checklist

Items you must coordinate with your TREP admin team:

- [ ] **DACS user created:** `marketdata-adapter` (or your chosen name)
- [ ] **MaxLogins ≥ 3:** allows the steady state (2 sessions) + headroom during failover (briefly 3)
- [ ] **Allow duplicate logins from same user:** enabled (DACS configurable)
- [ ] **Position whitelist:** both hall egress IPs (`10.10.1.100`, `10.10.2.100`) added to the user's allowed positions
- [ ] **Application ID:** assigned (we use `256` by default; verify it's licensed for your products)
- [ ] **Product permissions:** the DACS user is entitled to the services/RICs you'll subscribe (e.g. ELEKTRON_DD: equities, FX, fixed income as applicable)

## Verifying after deployment

Once deployed, check the active session count from ADS-side:

```bash
# Ask your TREP admin to run on the ADS:
adsadmin> show user marketdata-adapter
# Should show 2 active connections from positions 10.10.1.100 and 10.10.2.100
```

Or from our side via metrics:

```bash
oc exec market-data-service-0 -- curl -s localhost:8080/actuator/prometheus \
  | grep marketdata_lseg_session
# Expected: 1 per warm-eligible pod
```

## What happens during a failover spike

Briefly during cross-hall failover, you may see **3 sessions** for ~5–10s:

1. **t=0s:** hall1 leader crashes → its session lingers on ADS (TCP RST detected by ADS in ~30s ping timeout)
2. **t=5s:** hall2 warm pod becomes leader → uses its existing session #2 (no new session opened)
3. **t=10s:** hall1 cold pod (`hall1/pod-1`) starts up → opens a new warm session
4. **t=10–30s:** ADS still thinks hall1's old session is alive until its ping timeout expires
5. **t=30s:** ADS times out the dead session → back to 2 sessions

**Action:** set `MaxLogins ≥ 3` on the DACS user to accommodate this brief overlap. Set ADS `ConnectionPingTimeout = 30000` (matches our config) so dead sessions are reaped quickly.

## What we do NOT use

- ❌ **EMA `WarmStandbyChannelSet`** — this would make each pod open 2 connections (one to each hall's ADS), doubling our login count. We rely on application-level leader election instead.
- ❌ **Cloud RTO (OAuth2)** — the codebase supports it via `marketdata.lseg.connection-mode=RTO_CLOUD`, but on-prem TREP is the default for this deployment.

## References

- LSEG EMA Java Configuration Guide §3.4.4 (`RSSL_SOCKET` parameters) and §3.4.8 (`ChannelSet`)
- TREP DACS Admin Guide (request from your LSEG account manager)
- `src/main/java/com/example/marketdata/lseg/OmmConsumerManager.java` — see the `configureForOnPremTrep()` method for the actual auth wiring
