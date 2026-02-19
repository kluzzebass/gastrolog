-- Initial schema.
-- UUID format: 8-4-4-4-12 lowercase hex with hyphens, 36 chars total.

CREATE TABLE filters (
    id         TEXT PRIMARY KEY CHECK (length(id) = 36 AND id NOT GLOB '*[^0-9a-f-]*'),
    name       TEXT NOT NULL UNIQUE,
    expression TEXT NOT NULL
) STRICT;

CREATE TABLE rotation_policies (
    id          TEXT PRIMARY KEY CHECK (length(id) = 36 AND id NOT GLOB '*[^0-9a-f-]*'),
    name        TEXT NOT NULL UNIQUE,
    max_bytes   TEXT,
    max_age     TEXT,
    max_records INTEGER,
    cron        TEXT
) STRICT;

CREATE TABLE retention_policies (
    id         TEXT PRIMARY KEY CHECK (length(id) = 36 AND id NOT GLOB '*[^0-9a-f-]*'),
    name       TEXT NOT NULL UNIQUE,
    max_age    TEXT,
    max_bytes  TEXT,
    max_chunks INTEGER
) STRICT;

CREATE TABLE stores (
    id        TEXT PRIMARY KEY CHECK (length(id) = 36 AND id NOT GLOB '*[^0-9a-f-]*'),
    name      TEXT NOT NULL UNIQUE,
    type      TEXT NOT NULL,
    filter    TEXT CHECK (filter IS NULL OR (length(filter) = 36 AND filter NOT GLOB '*[^0-9a-f-]*')),
    policy    TEXT CHECK (policy IS NULL OR (length(policy) = 36 AND policy NOT GLOB '*[^0-9a-f-]*')),
    retention TEXT CHECK (retention IS NULL OR (length(retention) = 36 AND retention NOT GLOB '*[^0-9a-f-]*')),
    enabled   INTEGER NOT NULL DEFAULT 1,
    params    TEXT
) STRICT;

CREATE TABLE ingesters (
    id      TEXT PRIMARY KEY CHECK (length(id) = 36 AND id NOT GLOB '*[^0-9a-f-]*'),
    name    TEXT NOT NULL UNIQUE,
    type    TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    params  TEXT
) STRICT;

CREATE TABLE users (
    id            TEXT PRIMARY KEY CHECK (length(id) = 36 AND id NOT GLOB '*[^0-9a-f-]*'),
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'user',
    preferences   TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
) STRICT;

CREATE TABLE tls_certificates (
    id        TEXT PRIMARY KEY CHECK (length(id) = 36 AND id NOT GLOB '*[^0-9a-f-]*'),
    name      TEXT NOT NULL UNIQUE,
    cert_pem  TEXT,
    key_pem   TEXT,
    cert_file TEXT,
    key_file  TEXT
) STRICT;

CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
