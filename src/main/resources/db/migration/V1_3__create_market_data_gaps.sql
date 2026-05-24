-- ============================================================
--  V1.3  —  Table: market_data_gaps
--  Entity: com.example.marketdata.domain.MarketDataGapEntity
--
--  Persistent log of detected gaps. A gap is recorded when the new leader
--  detects that the time since last_published exceeds the threshold.
--  Each row corresponds to one GAP_DETECTED Kafka event on market-data-control.
-- ============================================================

CREATE TABLE IF NOT EXISTS market_data_gaps (
    id            BIGSERIAL    PRIMARY KEY,
    ric           VARCHAR(64)  NOT NULL,
    gap_start     TIMESTAMPTZ  NOT NULL,
    gap_end       TIMESTAMPTZ  NOT NULL,
    duration_ms   BIGINT       NOT NULL,
    detected_by   VARCHAR(64)  NOT NULL,
    published     BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  market_data_gaps IS 'Persistent gap log — one row per GAP_DETECTED Kafka event';
COMMENT ON COLUMN market_data_gaps.detected_by IS 'Pod name of the new leader that detected the gap (e.g. "market-data-service-0")';

-- History queries: gaps per RIC
CREATE INDEX IF NOT EXISTS idx_gaps_ric
    ON market_data_gaps (ric, created_at DESC);

-- Reconciliation: gaps that haven't been published yet
CREATE INDEX IF NOT EXISTS idx_gaps_unpublished
    ON market_data_gaps (published) WHERE published = FALSE;
