CREATE TABLE filters (
    filter_id  TEXT PRIMARY KEY,
    expression TEXT NOT NULL
) STRICT;

ALTER TABLE stores RENAME COLUMN route TO filter;
