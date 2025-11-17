# WorkQueue Worker (TypeScript)

Minimal Worker that consumes tasks from Redis list `task_queue` and processes by `type`. Exposes `/metrics` and `/dead_letter` endpoints.

## Prerequisites
- Node.js 20+
- Redis accessible at `REDIS_URL` (default `redis://127.0.0.1:6379/0`)

## Setup (Windows cmd)
```cmd
cd ts\worker
npm install
npm run dev
```

## Expected Output
When a task is enqueued (via Producer), the worker prints lines like:
```
Sending email to user@example.com with subject Hello
```

Stop with Ctrl+C.

## HTTP Endpoints
- `GET http://localhost:8081/metrics` → `{ total_jobs_in_queue, jobs_done, jobs_failed }`
- `GET http://localhost:8081/dead_letter?limit=50` → array of recent dead-lettered tasks
