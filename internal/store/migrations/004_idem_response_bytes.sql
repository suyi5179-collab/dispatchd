-- A replay must return the first response byte for byte. A JSON column
-- stores a normalized copy (keys reordered, spaces added), so the second
-- answer differed from the first. Keep the exact bytes instead.
-- @learn:idem-bytes
ALTER TABLE idempotency_keys MODIFY response MEDIUMBLOB NOT NULL;
-- @learn:end idem-bytes
