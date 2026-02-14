ALTER TABLE stores RENAME COLUMN paused TO enabled;
UPDATE stores SET enabled = CASE WHEN enabled = 0 THEN 1 ELSE 0 END;
