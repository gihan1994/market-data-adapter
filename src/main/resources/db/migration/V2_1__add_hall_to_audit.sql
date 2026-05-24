-- ============================================================
--  V2.1  —  Add hall column to subscription_audit
--
--  Lets us filter the audit log by OpenShift cluster (hall1 / hall2)
--  without parsing the free-form detail text. NULLABLE for backward
--  compatibility with pre-V2 rows.
-- ============================================================

ALTER TABLE subscription_audit
    ADD COLUMN IF NOT EXISTS hall VARCHAR(16);

COMMENT ON COLUMN subscription_audit.hall IS 'OpenShift cluster the pod is running in — "hall1" or "hall2"';

-- Filter audit history by hall
CREATE INDEX IF NOT EXISTS idx_audit_hall_time
    ON subscription_audit (hall, created_at DESC) WHERE hall IS NOT NULL;
