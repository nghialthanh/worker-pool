-- Bulk-insert 100_000 pending messages for stress testing.
-- Usage:
--   docker compose exec -T db psql -U app -d workerpool < docker/seed_100k.sql
-- Or from host (port 5433):
--   psql "host=localhost port=5433 user=app password=app dbname=workerpool" -f docker/seed_100k.sql

INSERT INTO messages (topic, message, status, created_at, updated_at)
SELECT
    'stress',
    'stress-bulk-' || g,
    0,          -- pending
    NOW(),
    NOW()
FROM generate_series(1, 100000) AS g;

-- Optional: bump to 500k by changing 100000 -> 500000 above.

SELECT status, COUNT(*) AS cnt
FROM messages
GROUP BY status
ORDER BY status;
