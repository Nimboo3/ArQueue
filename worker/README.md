# WorkQueue Worker (TypeScript)

Reliable worker consuming tasks from Redis lists using the BRPOPLPUSH pattern. Includes retries with exponential backoff, delayed scheduling via Sorted Set, crash recovery for stuck tasks, and a small dashboard.

## Prerequisites
- Node.js 20+
- Redis accessible at `REDIS_URL` (default `redis://127.0.0.1:6379/0`)

## Setup (Windows cmd)
```cmd
cd ts\worker
npm install
npm run dev
```

Stop with Ctrl+C. Graceful shutdown unblocks BRPOPLPUSH and waits for loops to exit.

## HTTP Endpoints
- `GET http://localhost:8081/metrics` → `{ concurrency, active_loops, jobs_done, jobs_failed, jobs_retried, queue_* }`
- `GET http://localhost:8081/dead_letter?limit=50` → array of recent dead-lettered tasks

## Environment Variables
- `REDIS_URL`: Redis connection string (default `redis://127.0.0.1:6379/0`)
- `PORT_WORKER`: HTTP port for metrics/dashboard (default `8081`)
- `WORKER_MAX_RETRIES`: Default max retries if task.maxRetries not set (default `3`)
- `DELAYED_SCAN_INTERVAL_MS`: Interval to move due retries from ZSET to pending (default `5000`)
- `PROCESSING_TIMEOUT_MS`: How long an item may sit in `queue:processing` before considered stuck (default `300000`)
- `PROCESSING_SCAN_INTERVAL_MS`: Interval to scan `queue:processing` for stuck items (default `10000`)
- `WORKER_CONCURRENCY`: Number of worker loops per process (default `4`)

## Notes on Reliability
- Removal and JSON serialization can race in rare cases. For production, prefer ID-only lists with task metadata in `task:<id>` hashes. This avoids JSON matching issues and enables atomic updates (HSET) for `startedAt` and status.
- Global metrics are stored in Redis counters (e.g., `metrics:jobs_done`) to aggregate across replicas; the `/metrics` endpoint also exposes per-process counters for quick debugging.
