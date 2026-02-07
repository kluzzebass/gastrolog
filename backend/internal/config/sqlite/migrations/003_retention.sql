CREATE TABLE retention_policies (
    retention_policy_id TEXT PRIMARY KEY,
    max_age             TEXT,
    max_bytes           TEXT,
    max_chunks          INTEGER
) STRICT;

ALTER TABLE stores ADD COLUMN retention TEXT;
