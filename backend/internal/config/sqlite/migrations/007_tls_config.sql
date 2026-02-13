CREATE TABLE tls_certificates (
    cert_id   TEXT PRIMARY KEY,
    cert_pem  TEXT NOT NULL DEFAULT '',
    key_pem   TEXT NOT NULL DEFAULT '',
    cert_file TEXT NOT NULL DEFAULT '',
    key_file  TEXT NOT NULL DEFAULT ''
) STRICT;

-- Remove TLS from generic settings (no longer stored there)
DELETE FROM settings WHERE key = 'tls';
