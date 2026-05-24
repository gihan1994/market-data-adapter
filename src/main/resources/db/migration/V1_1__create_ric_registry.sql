-- ============================================================
--  V1.1  —  Table: ric_registry
--  Entity: com.example.marketdata.domain.RicRegistryEntity
--
--  Durable list of all RICs the adapter has been asked to subscribe to.
--  PK is the RIC itself (one row per RIC, even across subscribers).
--  state column tracks the current lifecycle phase (see SubscriptionState enum).
-- ============================================================

CREATE TABLE IF NOT EXISTS ric_registry (
    ric                VARCHAR(64)   PRIMARY KEY,
    service_name       VARCHAR(64)   NOT NULL,
    active             BOOLEAN       NOT NULL DEFAULT TRUE,
    first_subscribed   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_state_change  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    state              VARCHAR(32)   NOT NULL DEFAULT 'REQUESTED'
        CHECK (state IN ('REQUESTED','SUBSCRIBING','ACTIVE','DRAINING','CLOSED','ERROR'))
);

COMMENT ON TABLE  ric_registry IS 'Durable RIC registry — one row per unique RIC';
COMMENT ON COLUMN ric_registry.state IS 'Lifecycle: REQUESTED → SUBSCRIBING → ACTIVE → DRAINING → CLOSED (or ERROR)';

-- Hot path: fetch all active RICs on leader promotion
CREATE INDEX IF NOT EXISTS idx_ric_registry_active
    ON ric_registry (active) WHERE active = TRUE;

-- Operational queries: filter by state
CREATE INDEX IF NOT EXISTS idx_ric_registry_state
    ON ric_registry (state);
