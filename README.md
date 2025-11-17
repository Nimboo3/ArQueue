# WorkQueue (TypeScript minimal)

Two services and a tiny dashboard.

- Producer: `POST /enqueue` to push tasks into Redis (`queue:pending`) with validation and rate limiting.
- Worker: Reliable dequeue with `BRPOPLPUSH`, retries with exponential backoff + jitter via `queue:delayed`, DLQ, `/metrics`, `/dead_letter`, and serves dashboard.

## Prerequisites
- Node.js 20+
- Redis running (default `redis://127.0.0.1:6379/0`).

## Quick start (Windows cmd)
```cmd
:: Producer
cd ts\producer
npm install
copy ..\.env.example .env
npm run dev

:: Worker (new terminal)
cd ts\worker
npm install
copy ..\.env.example .env
npm run dev
```

## Enqueue examples
```cmd
curl -X POST http://localhost:8080/enqueue ^
  -H "Content-Type: application/json" ^
  -d "{\"type\":\"send_email\",\"payload\":{\"to\":\"user@example.com\",\"subject\":\"Hello\"}}"

curl -X POST http://localhost:8080/enqueue ^
  -H "Content-Type: application/json" ^
  -d "{\"type\":\"unknown_task\",\"payload\":{}}"
```

## Inspect
- Metrics: `http://localhost:8081/metrics`
- Dead Letter: `http://localhost:8081/dead_letter?limit=20`
- Dashboard: open `http://localhost:8081/` in a browser
