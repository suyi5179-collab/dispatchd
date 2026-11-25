-- The outbox: written in the same transaction as the notification, so a
-- notification exists if and only if its Kafka message will be sent.
CREATE TABLE outbox (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  notification_id CHAR(30)     NOT NULL,
  topic           VARCHAR(128) NOT NULL,
  payload         JSON         NOT NULL,
  created_at      DATETIME(6)  NOT NULL,
  published_at    DATETIME(6)  NULL,
  kafka_partition INT          NULL,
  kafka_offset    BIGINT       NULL,
  INDEX idx_unpublished (published_at, id),
  INDEX idx_notification (notification_id)
);

-- A worker that claims an attempt holds it until lease_until. If the
-- worker dies mid-attempt, the lease runs out and the attempt is retried.
ALTER TABLE notifications ADD COLUMN lease_until DATETIME(6) NULL;
-- When the next attempt is due (chapter 04 schedules retries with it).
ALTER TABLE notifications ADD COLUMN next_attempt_at DATETIME(6) NULL;
CREATE INDEX idx_status_next ON notifications (status, next_attempt_at);
