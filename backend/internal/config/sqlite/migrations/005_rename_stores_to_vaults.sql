-- Rename stores table to vaults.
ALTER TABLE stores RENAME TO vaults;

-- Rename store_retention_rules to vault_retention_rules.
ALTER TABLE store_retention_rules RENAME TO vault_retention_rules;

-- Rename store_id column to vault_id in vault_retention_rules.
ALTER TABLE vault_retention_rules RENAME COLUMN store_id TO vault_id;
