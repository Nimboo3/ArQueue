import dotenv from 'dotenv';
import { Redis } from 'ioredis';
import express from 'express';
import { logger } from './logger.js';
import { fileURLToPath } from 'url';
import path from 'path';
import { randomUUID } from 'crypto';

// Resolve __dirname in ESM and load .env from local and repo-level fallback
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config();
dotenv.config({ path: path.resolve(__dirname, '../../.env'), override: false });

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const PORT = Number(process.env.PORT_WORKER || 8081);
const MAX_RETRIES = Number(process.env.WORKER_MAX_RETRIES || 3);
const DELAYED_SCAN_INTERVAL_MS = Number(process.env.DELAYED_SCAN_INTERVAL_MS || 5000);
const PROCESSING_TIMEOUT_MS = Number(process.env.PROCESSING_TIMEOUT_MS || 300000);
const PROCESSING_SCAN_INTERVAL_MS = Number(process.env.PROCESSING_SCAN_INTERVAL_MS || 10000);
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 4);   // NEW

const redis = new Redis(REDIS_URL);

interface Task {
  id?: string;
  type: string;
  payload: Record<string, unknown>;
  retries?: number;
  maxRetries?: number;
  lastError?: string | null;
  createdAt?: number;
  startedAt?: number | null;
}

let running = true;
let jobsDone = 0;
let jobsFailed = 0;
let jobsRetried = 0;

// Track active loops for graceful shutdown
const activeLoops = new Set<number>();   // NEW

function sleep(ms: number) {
  return new Promise(r => setTimeout(r, ms));
}

function ensureTaskDefaults(t: Task) {
  if (!t.id) t.id = randomUUID();
  if (typeof t.retries !== 'number') t.retries = 0;
  if (!t.createdAt) t.createdAt = Date.now();
  if (t.startedAt === undefined) t.startedAt = null;
  return t;
}

async function processTask(t: Task) {
  ensureTaskDefaults(t);
  switch (t.type) {
    case 'send_email': {
      logger.info({ taskId: t.id, to: t.payload['to'], subject: t.payload['subject'] }, 'processing send_email');
      await sleep(200);
      return;
    }
    case 'generate_pdf': {
      logger.info({ taskId: t.id }, 'processing generate_pdf');
      await sleep(100);
      return;
    }
    default:
      throw new Error('unsupported_task_type');
  }
}

// ----------------------
// WORKER LOOP WITH ID
// ----------------------
async function workerLoop(loopId: number) {   // NEW
  logger.info({ loopId }, 'Worker loop started');
  activeLoops.add(loopId);

  while (running) {
    try {
      const raw = await redis.brpoplpush('queue:pending', 'queue:processing', 1);
      if (!raw) continue;

      let task: Task;
      try {
        task = JSON.parse(raw);
      } catch {
        logger.error({ loopId, raw }, 'Invalid JSON in queue');
        await redis.lrem('queue:processing', 1, raw);
        continue;
      }

      task = ensureTaskDefaults(task);
      task.startedAt = Date.now();

      let currentRaw = JSON.stringify(task);

      try {
        await redis.lset('queue:processing', 0, currentRaw);
      } catch (e) {
        logger.warn({ loopId, err: e }, 'lset race');
      }

      try {
        await processTask(task);
        jobsDone++;
        await redis.lrem('queue:processing', 1, currentRaw);
        await redis.incr('metrics:jobs_done');
      } catch (err) {
        jobsFailed++;
        task.lastError = (err as Error).message;
        task.retries = (task.retries || 0) + 1;
        const max = task.maxRetries ?? MAX_RETRIES;

        await redis.lrem('queue:processing', 1, currentRaw);

        if (task.retries <= max) {
          jobsRetried++;
          const delay = backoffMs(task.retries);
          const runAt = Date.now() + delay;
          currentRaw = JSON.stringify(task);
          await redis.zadd('queue:delayed', runAt, currentRaw);
          logger.warn({ loopId, taskId: task.id, retries: task.retries }, 'scheduled retry');
        } else {
          currentRaw = JSON.stringify(task);
          await redis.lpush('queue:dead_letter', currentRaw);
          await redis.incr('metrics:jobs_dead_letter');
          logger.error({ loopId, taskId: task.id }, 'moved to DLQ');
        }
      }
    } catch (err) {
      logger.error({ loopId, err }, 'Worker loop error');
      await sleep(250);
    }
  }

  activeLoops.delete(loopId);            // NEW
  logger.info({ loopId }, 'Worker loop stopped');
}

// ----------------------
// START MULTIPLE LOOPS
// ----------------------
for (let i = 0; i < CONCURRENCY; i++) {      // NEW
  workerLoop(i).catch(err =>
    logger.error({ loopId: i, err }, 'Loop crashed')
  );
}

// ----------------------
// METRICS SERVER
// ----------------------

const app = express();
app.use(express.json({ limit: '64kb' }));

app.get('/metrics', async (_req, res) => {
  try {
    const pending = await redis.llen('queue:pending');
    const processing = await redis.llen('queue:processing');
    const dead = await redis.llen('queue:dead_letter');
    const delayed = await redis.zcard('queue:delayed');
    res.json({
      concurrency: CONCURRENCY,        // NEW
      active_loops: activeLoops.size,  // NEW
      total_jobs_in_queue: pending,
      jobs_done: jobsDone,
      jobs_failed: jobsFailed,
      jobs_retried: jobsRetried,
      queue_pending: pending,
      queue_processing: processing,
      queue_dead_letter: dead,
      queue_delayed: delayed
    });
  } catch {
    res.status(500).json({ error: 'metrics_error' });
  }
});

app.get('/dead_letter', async (req, res) => {
  const limit = Math.min(100, Number((req.query.limit as string) || 50));
  const items = await redis.lrange('queue:dead_letter', 0, limit - 1);
  res.json(items.map(i => {
    try { return JSON.parse(i); }
    catch { return { raw: i }; }
  }));
});

// Serve dashboard
try {
  const dashboardDir = path.resolve(__dirname, '../../dashboard');
  app.use(express.static(dashboardDir));
  logger.info({ dashboardDir }, 'Serving dashboard');
} catch (e) {
  logger.warn('Could not set up static dashboard');
}

const server = app.listen(PORT, () =>
  logger.info({ port: PORT }, 'Worker HTTP listening')
);

// ----------------------
// GRACEFUL SHUTDOWN
// ----------------------
async function shutdown(signal: string) {
  logger.info({ signal }, 'Stopping worker');

  running = false;

  // Wake up all brpoplpush calls
  try { 
    await redis.rpush('queue:pending', JSON.stringify({ __sentinel: true })); 
  } catch {}

  // Wait until all loops exit
  const interval = setInterval(() => {
    if (activeLoops.size === 0) {
      clearInterval(interval);
      server.close(() => process.exit(0));
    }
  }, 100);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// ----------------------
// DELAYED + RECOVERY
// ----------------------

function backoffMs(retryCount: number, base = 2000, max = 60000, jitterRatio = 0.1) {
  const exp = base * Math.pow(2, Math.max(0, retryCount - 1));
  const capped = Math.min(exp, max);
  const jitter = capped * jitterRatio * (Math.random() * 2 - 1);
  return Math.round(capped + jitter);
}

async function processDelayedBatch(limit = 100) {
  const now = Date.now();
  const items = await redis.zrangebyscore('queue:delayed', '-inf', now, 'LIMIT', 0, limit);
  if (items.length === 0) return;

  for (const raw of items) {
    await redis.rpush('queue:pending', raw);
    await redis.zrem('queue:delayed', raw);
  }
  logger.info({ moved: items.length }, 'delayed -> pending');
}

setInterval(() => {
  if (!running) return;
  processDelayedBatch().catch(err => logger.error({ err }, 'processDelayed error'));
}, DELAYED_SCAN_INTERVAL_MS);

async function recoverProcessing(limit = 1000) {
  if (!running) return;
  const now = Date.now();
  const timeout = PROCESSING_TIMEOUT_MS;

  const items = await redis.lrange('queue:processing', 0, limit - 1);
  if (items.length === 0) return;

  let recovered = 0;
  let sentToDlq = 0;

  for (const raw of items) {
    let task: Task;
    try { task = JSON.parse(raw); } catch { continue; }

    if (!task.startedAt) continue;
    if (now - task.startedAt < timeout) continue;

    const removed = await redis.lrem('queue:processing', 1, raw);
    if (removed === 0) continue;

    task.lastError = 'processing_timeout';
    task.retries = (task.retries || 0) + 1;

    const max = task.maxRetries ?? MAX_RETRIES;

    if (task.retries <= max) {
      const delay = backoffMs(task.retries);
      const runAt = Date.now() + delay;
      await redis.zadd('queue:delayed', runAt, JSON.stringify(task));
      recovered++;
    } else {
      await redis.lpush('queue:dead_letter', JSON.stringify(task));
      await redis.incr('metrics:jobs_dead_letter');
      sentToDlq++;
    }
  }

  if (recovered || sentToDlq) {
    logger.warn({ recovered, sentToDlq, timeout }, 'processing recovery scan');
  }
}

setInterval(() => {
  recoverProcessing().catch(err => logger.error({ err }, 'recoverProcessing error'));
}, PROCESSING_SCAN_INTERVAL_MS);
