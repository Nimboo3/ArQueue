import express from 'express';
import dotenv from 'dotenv';
import { Redis } from 'ioredis';
import rateLimit from 'express-rate-limit';
import { z } from 'zod';
import { randomUUID } from 'crypto';
import { logger } from './logger.js';

dotenv.config();

const app = express();

// Body limit to avoid huge payload abuse
app.use(express.json({ limit: '64kb' }));

// Simple request-id middleware
app.use((req, res, next) => {
  const reqId = req.header('X-Request-ID') || randomUUID();
  (req as any).requestId = reqId;
  res.setHeader('X-Request-ID', reqId);
  next();
});

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const PORT = Number(process.env.PORT_PRODUCER || 8080);

const redis = new Redis(REDIS_URL);

redis.on('error', err => logger.error({ err }, 'redis_error'));
redis.on('connect', () => logger.info('redis_connect'));
redis.on('reconnecting', () => logger.warn('redis_reconnecting'));

// Basic health endpoint
app.get('/healthz', (_req, res) => res.status(200).json({ status: 'ok' }));

// Task input schema
const TaskSchema = z.object({
  type: z.string().nonempty(),
  payload: z.record(z.any()),
  maxRetries: z.number().int().min(0).max(50).optional()
});

function isRecord(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === 'object' && !Array.isArray(v);
}

const limiter = rateLimit({
  windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS || 60_000),
  max: Number(process.env.RATE_LIMIT_MAX || 120),
  standardHeaders: true,
  legacyHeaders: false,
  handler: (_req, res) => res.status(429).json({ error: 'Too many requests' })
});

// Enqueue API
app.post('/enqueue', limiter, async (req, res) => {
  try {
    const parsed = TaskSchema.parse(req.body);

    // Example per-type payload check
    if (parsed.type === 'send_email') {
      if (!isRecord(parsed.payload) || typeof parsed.payload['to'] !== 'string' || typeof parsed.payload['subject'] !== 'string') {
        return res.status(400).json({ error: 'Bad request: payload must include "to" and "subject" strings' });
      }
    }

    const id = randomUUID();
    const task = {
      id,
      type: parsed.type,
      payload: parsed.payload,
      retries: 0,
      maxRetries: parsed.maxRetries ?? Number(process.env.WORKER_MAX_RETRIES ?? 3),
      createdAt: Date.now()
    };

    const raw = JSON.stringify(task);

    // Store metadata in a hash for robust updates / recovery
    // We store the original serialized data as 'data' too so recovery can reconstruct if needed
    await redis.hset(`task:${id}`, {
      data: raw,
      retries: String(task.retries),
      maxRetries: String(task.maxRetries),
      createdAt: String(task.createdAt),
      status: 'pending',
      type: task.type
    });

    // Enqueue the JSON payload for v1 (list stores JSON). In production we'd push just the id.
    const len = await redis.rpush('queue:pending', raw);

    logger.info({ taskId: id, type: task.type, requestId: (req as any).requestId }, 'task_enqueued');

    // Increment a global counter (aggregatable across processes)
    await redis.incr('metrics:jobs_enqueued');

    res.status(201).json({ taskId: id, queueLength: len, requestId: (req as any).requestId });
  } catch (err) {
    logger.error({ err }, 'enqueue_error');
    if (err instanceof z.ZodError) {
      return res.status(400).json({ error: err.errors.map(e => e.message).join('; ') });
    }
    res.status(500).json({ error: 'internal_server_error' });
  }
});

const server = app.listen(PORT, () => logger.info({ port: PORT }, 'producer_listening'));

// graceful shutdown
async function shutdown(signal: string) {
  try {
    logger.info({ signal }, 'producer_shutting_down');
    server.close(() => logger.info('producer_http_closed'));
    await redis.quit();
    process.exit(0);
  } catch (e) {
    logger.error({ e }, 'error during shutdown');
    process.exit(1);
  }
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
