import express from 'express';
import dotenv from 'dotenv';
import Redis from 'ioredis';
import rateLimit from 'express-rate-limit';
import { z } from 'zod';
import { randomUUID } from 'crypto';
import { logger } from './logger.js';

dotenv.config();

const app = express();
// Limit body size to avoid oversized payloads
app.use(express.json({ limit: '64kb' }));

// Request ID middleware (propagate or generate)
app.use((req, res, next) => {
  const reqId = req.header('X-Request-ID') || randomUUID();
  (req as any).requestId = reqId;
  res.setHeader('X-Request-ID', reqId);
  next();
});

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const PORT = Number(process.env.PORT_PRODUCER || 8080);

const redis = new Redis(REDIS_URL);

// Basic health endpoint
app.get('/healthz', (_req, res) => res.status(200).json({ status: 'ok' }));


const TaskSchema = z.object({
  type: z.string().nonempty(),
  payload: z.record(z.any()),
  maxRetries: z.number().int().min(0).max(10).optional()
});

function isRecord(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === 'object' && !Array.isArray(v);
}

const limiter = rateLimit({
  windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS || 60000),
  max: Number(process.env.RATE_LIMIT_MAX || 120),
  standardHeaders: true,
  legacyHeaders: false
});

app.post('/enqueue', limiter, async (req, res) => {
  try {
    // Content-Type safeguard
    if ((req.headers['content-type'] || '').split(';')[0] !== 'application/json') {
      return res.status(415).json({ error: 'Unsupported Media Type: use application/json' });
    }
    const parsed = TaskSchema.parse(req.body);
    if (parsed.type === 'send_email') {
      if (!isRecord(parsed.payload) || typeof parsed.payload['to'] !== 'string' || typeof parsed.payload['subject'] !== 'string') {
        return res.status(400).json({ error: 'Bad request, pass string payload fields: to, subject' });
      }
    }

    const task = {
      id: randomUUID(),
      type: parsed.type,
      payload: parsed.payload,
      retries: 0,
      createdAt: Date.now(),
      maxRetries: parsed.maxRetries ?? undefined
    };

    const serialized = JSON.stringify(task);
    const len = await redis.rpush('queue:pending', serialized);
    logger.info({ taskId: task.id, type: task.type, requestId: (req as any).requestId }, 'Task enqueued');
    // 201 Created is more semantically correct
    res.status(201).json({ taskId: task.id, queueLength: len, requestId: (req as any).requestId });
  } catch (err) {
    logger.error({ err }, 'enqueue error');
    if (err instanceof z.ZodError) {
      return res.status(400).json({ error: err.message });
    }
    res.status(500).json({ error: 'Internal server error' });
  }
});

const server = app.listen(PORT, () => {
  logger.info({ port: PORT }, 'Producer listening');
});

// Graceful shutdown (simple)
async function shutdown(signal: string) {
  logger.info({ signal }, 'Shutting down producer');
  server.close(() => process.exit(0));
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
