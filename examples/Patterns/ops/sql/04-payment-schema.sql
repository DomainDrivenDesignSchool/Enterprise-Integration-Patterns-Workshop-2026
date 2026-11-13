-- Run in database: payment. A provider adapter must honor idempotency_key.
CREATE TABLE IF NOT EXISTS payment_attempts (
  id uuid PRIMARY KEY, order_id uuid NOT NULL, idempotency_key text NOT NULL UNIQUE,
  amount numeric(18,2) NOT NULL CHECK(amount > 0), currency char(3) NOT NULL,
  status text NOT NULL CHECK(status IN ('Initiated','Unknown','Authorized','Rejected','ManualReview','Refunded')),
  provider_reference text NULL UNIQUE, decline_reason text NULL,
  created_at_utc timestamptz NOT NULL, updated_at_utc timestamptz NULL,
  next_reconcile_at_utc timestamptz NULL,
  reconciliation_attempts integer NOT NULL DEFAULT 0 CHECK(reconciliation_attempts >= 0),
  reconciliation_owner text NULL, reconciliation_locked_until_utc timestamptz NULL,
  last_error text NULL
);
-- Idempotent migration for databases created by earlier workshop versions.
ALTER TABLE payment_attempts ADD COLUMN IF NOT EXISTS next_reconcile_at_utc timestamptz NULL;
ALTER TABLE payment_attempts ADD COLUMN IF NOT EXISTS reconciliation_attempts integer NOT NULL DEFAULT 0;
ALTER TABLE payment_attempts ADD COLUMN IF NOT EXISTS reconciliation_owner text NULL;
ALTER TABLE payment_attempts ADD COLUMN IF NOT EXISTS reconciliation_locked_until_utc timestamptz NULL;
ALTER TABLE payment_attempts ADD COLUMN IF NOT EXISTS last_error text NULL;
ALTER TABLE payment_attempts DROP CONSTRAINT IF EXISTS payment_attempts_status_check;
ALTER TABLE payment_attempts ADD CONSTRAINT payment_attempts_status_check
  CHECK(status IN ('Initiated','Unknown','Authorized','Rejected','ManualReview','Refunded'));
CREATE INDEX IF NOT EXISTS ix_payment_reconciliation_due
  ON payment_attempts(next_reconcile_at_utc, created_at_utc)
  WHERE status IN ('Unknown','Initiated');
CREATE TABLE IF NOT EXISTS payment_ledger (
  id uuid PRIMARY KEY, order_id uuid NOT NULL, entry_type text NOT NULL CHECK(entry_type IN ('Charge','Refund')),
  amount numeric(18,2) NOT NULL, currency char(3) NOT NULL, reference text NOT NULL,
  created_at_utc timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_payment_ledger_order ON payment_ledger(order_id,created_at_utc);
-- A provider reference can produce at most one full charge and one full refund in this lab.
CREATE UNIQUE INDEX IF NOT EXISTS ux_payment_ledger_reference_type
  ON payment_ledger(reference, entry_type);

CREATE OR REPLACE FUNCTION prevent_payment_ledger_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'payment_ledger is append-only; UPDATE/DELETE is forbidden';
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_payment_ledger_append_only ON payment_ledger;
CREATE TRIGGER trg_payment_ledger_append_only
  BEFORE UPDATE OR DELETE ON payment_ledger
  FOR EACH ROW EXECUTE FUNCTION prevent_payment_ledger_mutation();
CREATE TABLE IF NOT EXISTS payment_outbox (
  id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
  created_at_utc timestamptz NOT NULL, published_at_utc timestamptz NULL,
  lock_owner text NULL, locked_until_utc timestamptz NULL
);

CREATE TABLE IF NOT EXISTS payment_refund_attempts (
  id uuid PRIMARY KEY, order_id uuid NOT NULL, payment_reference text NOT NULL,
  idempotency_key text NOT NULL UNIQUE, amount numeric(18,2) NOT NULL CHECK(amount > 0),
  currency char(3) NOT NULL,
  status text NOT NULL CHECK(status IN ('Initiated','Unknown','Refunded','Rejected','ManualReview')),
  reconciliation_attempts integer NOT NULL DEFAULT 0 CHECK(reconciliation_attempts >= 0),
  next_reconcile_at_utc timestamptz NULL, reconciliation_owner text NULL,
  reconciliation_locked_until_utc timestamptz NULL, last_error text NULL,
  created_at_utc timestamptz NOT NULL, updated_at_utc timestamptz NULL
);
CREATE INDEX IF NOT EXISTS ix_payment_refund_reconciliation_due
  ON payment_refund_attempts(next_reconcile_at_utc, created_at_utc)
  WHERE status IN ('Unknown','Initiated');

CREATE INDEX IF NOT EXISTS ix_payment_outbox_pending ON payment_outbox(created_at_utc) WHERE published_at_utc IS NULL;
