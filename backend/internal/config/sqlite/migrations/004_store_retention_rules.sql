-- Replace single retention column with a junction table supporting multiple
-- retention rules per store, each with an action (expire or migrate).

CREATE TABLE store_retention_rules (
    store_id            TEXT NOT NULL CHECK (length(store_id) = 36),
    retention_policy_id TEXT NOT NULL CHECK (length(retention_policy_id) = 36),
    action              TEXT NOT NULL DEFAULT 'expire' CHECK (action IN ('expire', 'migrate')),
    destination_id      TEXT CHECK (destination_id IS NULL OR (length(destination_id) = 36)),
    CHECK (action != 'migrate' OR destination_id IS NOT NULL)
) STRICT;

-- Migrate existing single-policy references.
INSERT INTO store_retention_rules (store_id, retention_policy_id, action)
SELECT id, retention, 'expire'
FROM stores
WHERE retention IS NOT NULL;

-- Drop the old column.
ALTER TABLE stores DROP COLUMN retention;
