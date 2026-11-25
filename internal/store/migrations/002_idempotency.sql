-- One row per (caller, Idempotency-Key). The primary key is the whole
-- point: two concurrent requests with the same key cannot both insert,
-- so the database (not a read-then-write in Go) decides who is first.
CREATE TABLE idempotency_keys (
  client_id       VARCHAR(64)  NOT NULL,
  idem_key        VARCHAR(255) NOT NULL,
  request_hash    CHAR(64)     NOT NULL,
  notification_id CHAR(30)     NOT NULL,
  status_code     INT          NOT NULL,
  -- @learn:idem-json-column
  response        JSON         NOT NULL,
  -- @learn:end idem-json-column
  created_at      DATETIME(6)  NOT NULL,
  PRIMARY KEY (client_id, idem_key),
  INDEX idx_created (created_at)
);
