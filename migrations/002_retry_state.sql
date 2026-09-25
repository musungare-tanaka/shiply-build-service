ALTER TABLE build_service.stage_ledger
  ADD COLUMN IF NOT EXISTS retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  ADD COLUMN IF NOT EXISTS last_error text;
