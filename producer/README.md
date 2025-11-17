# WorkQueue Producer (TypeScript)

Minimal Producer API mirroring the Go service. Provides `POST /enqueue` to push tasks into Redis list `task_queue`.

## Prerequisites
- Node.js 20+
- Redis accessible at `REDIS_URL` (default `redis://127.0.0.1:6379/0`)

## Setup (Windows cmd)
```cmd
cd ts\producer
npm install
copy ..\.env.example .env
npm run dev
```

## Health Check
```cmd
curl http://localhost:8080/healthz
```

## Enqueue Example
```cmd
curl -X POST http://localhost:8080/enqueue ^
  -H "Content-Type: application/json" ^
  -d "{\"type\":\"send_email\",\"payload\":{\"to\":\"user@example.com\",\"subject\":\"Hello\"},\"retries\":3}"
```

Expected response:
```
Task of type 'send_email' has been successfully added to the queue (len=<n>)
```
