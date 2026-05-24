-- ============================================================
--  V1.4  —  Table: subscription_audit
--  Entity: com.example.marketdata.domain.SubscriptionAuditEntity
--
--  Append-only audit log. Records pod lifecycle events (POD_STARTED,
--  POD_TERMINATING, LEADER_ACQUIRED/LOST), per-RIC state changes,
--  LSEG connection events, and gap events. Used for incident forensics
--  and compliance.
-- ============================================================

CREATE TABLE IF NOT EXISTS subscription_audit (
    id          BIGSERIAL     PRIMARY KEY,
    pod_name    VARCHAR(64)   NOT NULL,
    ric         VARCHAR(64),
    event_type  VARCHAR(32)   NOT NULL,
    detail      TEXT,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  subscription_audit IS 'Append-only audit log — see AuditEventType enum for the set of event_type values';
COMMENT ON COLUMN subscription_audit.event_type IS 'POD_STARTED, LEADER_ACQUIRED, LEADER_LOST, RIC_SUBSCRIBED, RIC_UNSUBSCRIBED, RIC_STATE_CHANGED, GAP_DETECTED, etc.';

-- Per-pod timeline (most common query)
CREATE INDEX IF NOT EXISTS idx_audit_pod_time
    ON subscription_audit (pod_name, created_at DESC);

-- Filter by event type
CREATE INDEX IF NOT EXISTS idx_audit_event
    ON subscription_audit (event_type, created_at DESC);

-- Per-RIC timeline (skip null rics)
CREATE INDEX IF NOT EXISTS idx_audit_ric
    ON subscription_audit (ric, created_at DESC) WHERE ric IS NOT NULL;
