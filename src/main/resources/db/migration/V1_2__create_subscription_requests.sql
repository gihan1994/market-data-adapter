-- ============================================================
--  V1.2  —  Table: subscription_requests
--  Entity: com.example.marketdata.domain.SubscriptionRequestEntity
--
--  Tracks which business MS asked for which RIC. Refcount per RIC is
--  count of rows where active = true. This is the durable backing of
--  the Redis 'subscription:refcount:*' counters.
-- ============================================================

CREATE TABLE IF NOT EXISTS subscription_requests (
    id            BIGSERIAL     PRIMARY KEY,
    business_ms   VARCHAR(64)   NOT NULL,
    ric           VARCHAR(64)   NOT NULL,
    requested_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    active        BOOLEAN       NOT NULL DEFAULT TRUE,
    UNIQUE (business_ms, ric)
);

COMMENT ON TABLE  subscription_requests IS 'Per-business-MS subscription records — refcount source of truth';
COMMENT ON COLUMN subscription_requests.business_ms IS 'Identifier from the SubscriptionRequest.requester field (e.g. "lcm-ms")';

-- Refcount lookup: count(*) WHERE ric = ? AND active = true
CREATE INDEX IF NOT EXISTS idx_sub_req_ric
    ON subscription_requests (ric) WHERE active = TRUE;

-- Operational query: all RICs a specific MS is subscribed to
CREATE INDEX IF NOT EXISTS idx_sub_req_business_ms
    ON subscription_requests (business_ms) WHERE active = TRUE;
