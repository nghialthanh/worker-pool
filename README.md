# worker-pool

Go worker pool that claims pending rows from Postgres and processes them concurrently.

## Stack

- **App**: `log/slog` JSON logs, GORM + Postgres
- **Docker**: Postgres 16 + [PostgREST](https://postgrest.org) on `:3000` for HTTP stress seeding

## Quick start

```bash
# 1) Start Postgres + PostgREST
docker compose up -d

# 2) Install deps & run workers
go mod tidy
MAX_WORKERS=8 TOPIC=stress go run .

# 3) Seed load via PostgREST (another terminal)
chmod +x scripts/stress_seed.sh
COUNT=500 TOPIC=stress ./scripts/stress_seed.sh
```

Env knobs:

| Var | Default | Meaning |
|-----|---------|---------|
| `DATABASE_URL` | `localhost:5433` docker DSN | Postgres connection |
| `MAX_WORKERS` | `8` | consumer goroutines |
| `TOPIC` | `stress` | claim filter (`""` = all topics) |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` |

## Useful PostgREST checks

```bash
# pending count header
curl -sI 'http://localhost:3000/messages?status=eq.0' -H 'Prefer: count=exact' | grep -i content-range

# success count
curl -sI 'http://localhost:3000/messages?status=eq.2' -H 'Prefer: count=exact' | grep -i content-range
```

Status values: `0` pending, `1` in_queue, `2` success, `3` failed.
