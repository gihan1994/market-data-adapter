-- ============================================================
--  V2.2  —  Add hall column to market_data_gaps
--
--  Identifies which OpenShift cluster's leader detected the gap.
--  Useful for analyzing cross-hall failover events specifically.
-- ============================================================

ALTER TABLE market_data_gaps
    ADD COLUMN IF NOT EXISTS hall VARCHAR(16);

COMMENT ON COLUMN market_data_gaps.hall IS 'Hall of the leader that detected the gap — "hall1" or "hall2"';

CREATE INDEX IF NOT EXISTS idx_gaps_hall
    ON market_data_gaps (hall, created_at DESC) WHERE hall IS NOT NULL;
