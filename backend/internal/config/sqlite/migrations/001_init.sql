CREATE TABLE rotation_policies (
    rotation_policy_id TEXT PRIMARY KEY,
    max_bytes          TEXT,
    max_age            TEXT,
    max_records        INTEGER
) STRICT;

CREATE TABLE stores (
    store_id TEXT PRIMARY KEY,
    type     TEXT NOT NULL,
    route    TEXT,
    policy   TEXT,
    params   TEXT
) STRICT;

CREATE TABLE ingesters (
    ingester_id TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    params      TEXT
) STRICT;
