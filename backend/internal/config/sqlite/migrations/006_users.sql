CREATE TABLE users (
    username      TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'user',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
) STRICT;
