# ArQueue - Distributed Task Queue

This is a  Redis-backed task queue implemented in TypeScript.
It demonstrates real-world async patterns:
reliable dequeue, retries with exponential backoff + jitter, delayed queue,
dead-letter queue (DLQ), crash recovery, structured logging, and concurrency.

<img width="2608" height="783" alt="queue" src="https://github.com/user-attachments/assets/37627f0c-6ea6-4692-8553-46eabb05b52b" />

--

## Key features

- Producer service: `POST /enqueue` accepts validated tasks and enqueues them.
- Workers: multiple concurrent worker loops per process (configurable via `WORKER_CONCURRENCY`) consume tasks.
- Reliable dequeue: uses `BRPOPLPUSH` from `queue:pending` to `queue:processing`.
- Per-task metadata stored in Redis hash: `task:<id>` stores `data`, `retries`, `startedAt`, `status`, etc.
  - This avoids unsafe `LSET` updates on list elements and helps recovery.
- Retry strategy: exponential backoff with jitter; delayed scheduling via `ZADD queue:delayed`.
- Dead-letter queue: exhausted tasks -> `queue:dead_letter`.
- Recovery scan: periodically scans `queue:processing` and uses task hash `startedAt` to decide stale tasks.
- Concurrency: `WORKER_CONCURRENCY` worker loops per process; horizontally scale by running multiple processes/containers.
- Observability: structured logs (Pino) and `/metrics` endpoint with aggregated counters (stored in Redis).
- Graceful shutdown: short BRPOP timeout + running flag + wait for loops to exit; clean Redis disconnect.
- Basic protections: rate-limiter on producer, request-id propagation, body size limits, Redis reconnect handlers.

--

## Quick start (local)

1. Create `.env` (or copy `.env.example`):

```env
REDIS_URL=redis://localhost:6379/0

PORT_PRODUCER=8080
PORT_WORKER=8081

WORKER_CONCURRENCY=3
WORKER_MAX_RETRIES=3

RATE_LIMIT_WINDOW_MS=60000
RATE_LIMIT_MAX=120

2. Start Redis (local or Docker). For quick local test:

docker run --rm -p 6379:6379 redis:7-alpine


3. Install & start producer and worker (example using two shells):

# root of project
cd producer
npm install
npm run dev   # or npm start after build

cd ../worker
npm install
npm run dev


4. Enqueue a task:

curl -X POST http://localhost:8080/enqueue \
  -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":{"to":"user@example.com","subject":"hi"}}'


5. Check worker metrics:

GET http://localhost:8081/metrics
GET http://localhost:8081/dead_letter
