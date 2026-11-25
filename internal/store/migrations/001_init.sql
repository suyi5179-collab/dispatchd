-- Callers authenticate with an API key; only its SHA-256 is stored.
CREATE TABLE api_clients (
  id         VARCHAR(64) PRIMARY KEY,
  key_hash   CHAR(64)    NOT NULL UNIQUE,
  role       VARCHAR(16) NOT NULL DEFAULT 'client',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

-- One row per notification: what to send and where it stands.
CREATE TABLE notifications (
  id           CHAR(30)     PRIMARY KEY,
  client_id    VARCHAR(64)  NOT NULL,
  channel      VARCHAR(8)   NOT NULL,
  recipient    VARCHAR(320) NOT NULL,
  template     VARCHAR(64)  NOT NULL,
  params       JSON         NOT NULL,
  status       VARCHAR(24)  NOT NULL,
  attempts     INT          NOT NULL DEFAULT 0,
  last_error   JSON         NULL,
  created_at   DATETIME(6)  NOT NULL,
  updated_at   DATETIME(6)  NOT NULL,
  delivered_at DATETIME(6)  NULL,
  INDEX idx_client_created (client_id, created_at),
  INDEX idx_status (status)
);

-- Every delivery attempt, kept for the caller and for the runbook.
CREATE TABLE delivery_attempts (
  notification_id CHAR(30)    NOT NULL,
  attempt         INT         NOT NULL,
  provider        VARCHAR(16) NOT NULL,
  started_at      DATETIME(6) NOT NULL,
  finished_at     DATETIME(6) NULL,
  outcome         VARCHAR(24) NOT NULL,
  http_status     INT         NOT NULL DEFAULT 0,
  provider_code   VARCHAR(64) NOT NULL DEFAULT '',
  detail          JSON        NULL,
  latency_ms      DOUBLE      NOT NULL DEFAULT 0,
  PRIMARY KEY (notification_id, attempt)
);

-- An append-only log of what happened to a notification (status
-- transitions and the data each stage saw). The learn page reads it.
CREATE TABLE notification_events (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  notification_id CHAR(30)    NOT NULL,
  stage           VARCHAR(32) NOT NULL,
  from_status     VARCHAR(24) NOT NULL DEFAULT '',
  to_status       VARCHAR(24) NOT NULL DEFAULT '',
  data            JSON        NULL,
  at              DATETIME(6) NOT NULL,
  INDEX idx_notification (notification_id, id)
);
