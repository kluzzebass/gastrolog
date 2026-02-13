-- Ensure tls_certificates exists (007 may have been applied with old schema).
CREATE TABLE IF NOT EXISTS tls_certificates (
    cert_id   TEXT PRIMARY KEY,
    cert_pem  TEXT NOT NULL DEFAULT '',
    key_pem   TEXT NOT NULL DEFAULT '',
    cert_file TEXT NOT NULL DEFAULT '',
    key_file  TEXT NOT NULL DEFAULT ''
) STRICT;

-- Clean up obsolete tables from prior 007 variants.
DROP TABLE IF EXISTS tls_config;
DROP TABLE IF EXISTS tls_settings;

DELETE FROM settings WHERE key = 'tls';
