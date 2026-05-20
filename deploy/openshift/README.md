# OpenShift manifests

This directory contains the Kubernetes/OpenShift YAML for deploying `market-data-service`.

| File | Purpose |
|---|---|
| `statefulset.yaml` | 4-replica StatefulSet + headless service |
| `service.yaml` | ClusterIP service for HTTP/actuator |
| `configmap.yaml` | Kafka/Redis endpoints + per-pod role assignment |
| `secret.template.yaml` | Template for DB/Kafka/Redis/LSEG secrets — **do not commit real values** |
| `pdb.yaml` | PodDisruptionBudget — at most 1 pod down during voluntary disruption |

## Full deployment guide

See **[../../docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md)** for step-by-step instructions, verification, failover testing, scaling, and rollback procedures.

## Quick apply

```bash
oc new-project marketdata
oc apply -f configmap.yaml
# Fill in secret.template.yaml first; save as secret.yaml (gitignored)
oc apply -f secret.yaml
oc apply -f statefulset.yaml -f service.yaml -f pdb.yaml
```
