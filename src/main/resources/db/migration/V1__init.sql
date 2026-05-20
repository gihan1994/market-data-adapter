-- ============================================================
--  market-data-service initial schema
-- ============================================================

CREATE TABLE IF NOT EXISTS ric_registry (
    ric              VARCHAR(64)   PRIMARY KEY,
    service_name     VARCHAR(64)   NOT NULL,
    active           BOOLEAN       NOT NULL DEFAULT TRUE,
    first_subscribed TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_state_change TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    state            VARCHAR(32)   NOT NULL DEFAULT 'REQUESTED'
);

CREATE INDEX IF NOT EXISTS idx_ric_registry_active ON ric_registry (active) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_ric_registry_state  ON ric_registry (state);

CREATE TABLE IF NOT EXISTS subscription_requests (
    id            BIGSERIAL     PRIMARY KEY,
    business_ms   VARCHAR(64)   NOT NULL,
    ric           VARCHAR(64)   NOT NULL,
    requested_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    active        BOOLEAN       NOT NULL DEFAULT TRUE,
    UNIQUE (business_ms, ric)
);

CREATE INDEX IF NOT EXISTS idx_sub_req_ric         ON subscription_requests (ric) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_sub_req_business_ms ON subscription_requests (business_ms) WHERE active = TRUE;

CREATE TABLE IF NOT EXISTS market_data_gaps (
    id           BIGSERIAL    PRIMARY KEY,
    ric          VARCHAR(64)  NOT NULL,
    gap_start    TIMESTAMPTZ  NOT NULL,
    gap_end      TIMESTAMPTZ  NOT NULL,
    duration_ms  BIGINT       NOT NULL,
    detected_by  VARCHAR(64)  NOT NULL,
    published    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gaps_ric         ON market_data_gaps (ric, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_gaps_unpublished ON market_data_gaps (published) WHERE published = FALSE;

CREATE TABLE IF NOT EXISTS subscription_audit (
    id          BIGSERIAL     PRIMARY KEY,
    pod_name    VARCHAR(64)   NOT NULL,
    ric         VARCHAR(64),
    event_type  VARCHAR(32)   NOT NULL,
    detail      TEXT,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_pod_time  ON subscription_audit (pod_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event     ON subscription_audit (event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_ric       ON subscription_audit (ric, created_at DESC) WHERE ric IS NOT NULL;
