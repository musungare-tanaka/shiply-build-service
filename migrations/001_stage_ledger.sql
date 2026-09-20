CREATE SCHEMA IF NOT EXISTS build_service;
CREATE TABLE IF NOT EXISTS build_service.stage_ledger (
  deployment_id text NOT NULL,
  stage text NOT NULL,
  state text NOT NULL CHECK (state IN ('IN_PROGRESS','COMPLETED','FAILED')),
  attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
  lease_expires_at timestamptz,
  result_json jsonb,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, stage)
);
