-- Werken idempotency table (Postgres).
--
-- This library never runs DDL. Copy this into your own migration pipeline so the schema stays
-- under your control and your migration history stays truthful.
--
-- If you configure `idempotency.table`, rename the table and the index below to match.

CREATE TABLE IF NOT EXISTS werken_processed_events (
  consumer     text        NOT NULL,
  source       text        NOT NULL,
  event_id     text        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  expires_at   timestamptz NOT NULL,
  PRIMARY KEY (consumer, source, event_id)
);

-- `consumer` is part of the primary key so two services consuming the same event each process it
-- once, rather than once between them. `source` is part of it because `ce-id` is only unique
-- within a source.

-- Supports the expiry filter on reads and the pruning delete below.
CREATE INDEX IF NOT EXISTS werken_processed_events_expires_at_idx
  ON werken_processed_events (expires_at);

-- Pruning is your job, not the library's — a library that quietly issues DELETEs against your
-- database is a surprise nobody wants. Run this from a scheduled job:
--
--   DELETE FROM werken_processed_events WHERE expires_at < now();
--
-- `pruneExpiredSql(table?)` returns exactly that statement if you would rather not hardcode it.
