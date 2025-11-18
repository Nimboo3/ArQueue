import dotenv from 'dotenv';
import { Redis } from 'ioredis';
import express from 'express';
import { logger } from './logger.js';
import { fileURLToPath } from 'url';
import path from 'path';
import { randomUUID } from 'crypto';

dotenv.config();

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const PORT = Number(process.env.PORT_WORKER || 8081);
const MAX_RETRIES = Number(process.env.WORKER_MAX_RETRIES || 3);
const DELAYED_SCAN_INTERVAL_MS = Number(process.env.DELAYED_SCAN_INTERVAL_MS || 5000);
const PROCESSING_TIMEOUT_MS = Number(process.env.PROCESSING_TIMEOUT_MS || 300_000); // 5m
const PROCESSING_SCAN_INTERVAL_MS = Number(process.env.PROCESSING_SCAN_INTERVAL_MS || 10_000);
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 3);
const BRPOP_TIMEOUT = Number(process.env.BRPOP_TIMEOUT || 1); // seconds

const redis = new Redis(REDIS_URL);
redis.on('error', err => logger.error({ err }, 'redis_error'));
redis.on('connect', () => logger.info('redis_connected'));
redis.on('reconnecting', () => logger.warn('redis_reconnecting'));

// Task shape
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
// local counters (useful for quick dev; Redis counters are authoritative)
let jobsDoneLocal = 0;
let jobsFailedLocal = 0;
let jobsRetriedLocal = 0;

// track active loop ids for graceful shutdown
const activeLoops = new Set<number>();

function sleep(ms: number) { return new Promise(res => setTimeout(res, ms)); }

function ensureTaskDefaults(t: Task) {
  if (!t.id) t.id = randomUUID();
  if (typeof t.retries !== 'number') t.retries = 0;
  if (!t.maxRetries) t.maxRetries = MAX_RETRIES;
  if (!t.createdAt) t.createdAt = Date.now();
  if (t.startedAt === undefined) t.startedAt = null;
  return t;
}

async function processTask(t: Task) {
  ensureTaskDefaults(t);
  switch (t.type) {
    case 'send_email': {
      logger.info({ taskId: t.id, to: t.payload['to'], subject: t.payload['subject'] }, 'processing_send_email');
      // simulate I/O-bound work
      await sleep(200);
      return;
    }
    case 'generate_pdf': {
      logger.info({ taskId: t.id }, 'processing_generate_pdf');
      await sleep(100);
      return;
    }
    default:
      throw new Error('unsupported_task_type');
  }
}

// Produce the serialized JSON for storing in delayed / dead queues when needed
function serializeTask(t: Task) {
  return JSON.stringify({
    id: t.id,
    type: t.type,
    payload: t.payload,
    retries: t.retries,
    maxRetries: t.maxRetries,
    createdAt: t.createdAt,
    lastError: t.lastError,
    startedAt: t.startedAt
  });
}

// Backoff with jitter
function backoffMs(retryCount: number, base = 2000, max = 60_000, jitterRatio = 0.1) {
  const exp = base * Math.pow(2, Math.max(0, retryCount - 1));
  const capped = Math.min(exp, max);
  const jitter = capped * jitterRatio * (Math.random() * 2 - 1);
  return Math.round(capped + jitter);
}

// Worker loop - each loop competes for tasks
async function workerLoop(loopId: number) {
  logger.info({ loopId }, 'worker_loop_start');
  activeLoops.add(loopId);

  while (running) {
    try {
      // BRPOPLPUSH: pending -> processing (blocking)
      const raw = await redis.brpoplpush('queue:pending', 'queue:processing', BRPOP_TIMEOUT);
      if (!raw) continue;

      let task: Task;
      try {
        task = JSON.parse(raw);
      } catch (e) {
        logger.error({ loopId, raw }, 'invalid_json_in_pending');
        // remove the junk item from processing if present
        await redis.lrem('queue:processing', 1, raw);
        continue;
      }

      task = ensureTaskDefaults(task);

      // Update metadata in hash (atomic-ish per HSET) - avoids lset race
      const taskKey = `task:${task.id}`;
      try {
        await redis.hset(taskKey, {
          startedAt: String(Date.now()),
          status: 'processing',
          // ensure data field exists in case producer didn't set it
          data: raw
        });
      } catch (e) {
        logger.warn({ loopId, taskId: task.id, err: e }, 'failed_setting_task_hash');
      }

      // Process
      try {
        await processTask(task);

        // remove from processing list
        await redis.lrem('queue:processing', 1, raw);

        // mark completed in hash and set TTL for cleanup
        await redis.hset(taskKey, 'status', 'completed', 'completedAt', String(Date.now()));
        await redis.expire(taskKey, 7 * 24 * 60 * 60); // 7 days

        jobsDoneLocal += 1;
        await redis.incr('metrics:jobs_done');

        logger.info({ loopId, taskId: task.id }, 'task_completed');
      } catch (err) {
        // Failure path
        jobsFailedLocal += 1;
        const errMsg = (err instanceof Error) ? err.message : String(err);
        logger.error({ loopId, taskId: task.id, err: errMsg }, 'task_processor_error');

        // Update retries in hash atomically
        const newRetries = await redis.hincrby(taskKey, 'retries', 1);
        await redis.hset(taskKey, 'lastError', errMsg);

        const max = Number((await redis.hget(taskKey, 'maxRetries')) ?? task.maxRetries ?? MAX_RETRIES);

        // remove from processing list (we only remove one occurrence)
        await redis.lrem('queue:processing', 1, raw);

        if (newRetries <= max) {
          jobsRetriedLocal += 1;
          await redis.incr('metrics:jobs_retried');

          // Build updated serialized task (for scheduling)
          const updatedTask: Task = {
            ...task,
            retries: newRetries,
            lastError: errMsg,
            startedAt: null
          };

          const nextRun = Date.now() + backoffMs(newRetries);
          const serialized = serializeTask(updatedTask);
          await redis.zadd('queue:delayed', nextRun, serialized);

          // Update 'data' in hash to latest serialized form for visibility
          await redis.hset(taskKey, 'data', serialized, 'status', 'scheduled');
          logger.warn({ loopId, taskId: task.id, retries: newRetries }, 'scheduled_retry');
        } else {
          // Exhausted - move to DLQ
          const exhaustedTask = {
            ...task,
            retries: newRetries,
            lastError: errMsg,
            startedAt: null
          };
          const serialized = serializeTask(exhaustedTask);
          await redis.lpush('queue:dead_letter', serialized);
          await redis.hset(taskKey, 'status', 'failed', 'lastError', errMsg);
          await redis.incr('metrics:jobs_dead_letter');

          logger.error({ loopId, taskId: task.id }, 'moved_to_dlq');
        }
      }
    } catch (err) {
      logger.error({ loopId, err }, 'worker_loop_error');
      // small backoff to avoid tight error loops
      await sleep(250);
    }
  }

  activeLoops.delete(loopId);
  logger.info({ loopId }, 'worker_loop_exit');
}

// spawn multiple loops (concurrency)
for (let i = 0; i < CONCURRENCY; i++) {
  // fire and forget each loop; errors are logged inside
  workerLoop(i).catch(err => logger.error({ loopId: i, err }, 'loop_crashed'));
}

// Minimal HTTP server for metrics and DLQ inspection
const app = express();
app.use(express.json({ limit: '64kb' }));

app.get('/metrics', async (_req, res) => {
  try {
    const [pending, processing, dead, delayed, jobsDone, jobsFailed, jobsRetried, jobsDead] =
      await Promise.all([
        redis.llen('queue:pending'),
        redis.llen('queue:processing'),
        redis.llen('queue:dead_letter'),
        redis.zcard('queue:delayed'),
        redis.get('metrics:jobs_done'),
        redis.get('metrics:jobs_failed'),
        redis.get('metrics:jobs_retried'),
        redis.get('metrics:jobs_dead_letter')
      ]);

    res.json({
      concurrency: CONCURRENCY,
      active_loops: activeLoops.size,
      total_jobs_in_queue: pending,
      queue_processing: processing,
      queue_delayed: delayed,
      queue_dead_letter: dead,
      jobs_done: Number(jobsDone ?? jobsDoneLocal),
      jobs_failed: Number(jobsFailed ?? jobsFailedLocal),
      jobs_retried: Number(jobsRetried ?? jobsRetriedLocal),
      jobs_dead_letter: Number(jobsDead ?? 0)
    });
  } catch (err) {
    logger.error({ err }, 'metrics_error');
    res.status(500).json({ error: 'metrics_error' });
  }
});

app.get('/dead_letter', async (req, res) => {
  try {
    const limit = Math.min(100, Number((req.query.limit as string) || 50));
    const items = await redis.lrange('queue:dead_letter', 0, limit - 1);
    const parsed = items.map(i => {
      try { return JSON.parse(i); } catch { return { raw: i }; }
    });
    res.json(parsed);
  } catch (err) {
    logger.error({ err }, 'dead_letter_error');
    res.status(500).json({ error: 'dead_letter_error' });
  }
});

// Serve dashboard static if present
try {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const dashboardDir = path.resolve(__dirname, '../../dashboard');
  app.use(express.static(dashboardDir));
  logger.info({ dashboardDir }, 'serving_dashboard');
} catch (e) {
  logger.warn('dashboard_not_found');
}

const server = app.listen(PORT, () => {
  logger.info({ port: PORT }, 'worker_http_listening');
});

// delay mover: move due jobs from delayed -> pending
async function processDelayedBatch(limit = 100) {
  const now = Date.now();
  const items = await redis.zrangebyscore('queue:delayed', '-inf', now, 'LIMIT', 0, limit);
  if (items.length === 0) return;
  for (const raw of items) {
    // move back to pending and remove from delayed
    await redis.rpush('queue:pending', raw);
    await redis.zrem('queue:delayed', raw);
  }
  logger.info({ moved: items.length }, 'delayed_to_pending');
}

setInterval(() => {
  if (!running) return;
  processDelayedBatch().catch(err => logger.error({ err }, 'processDelayed_error'));
}, DELAYED_SCAN_INTERVAL_MS);

// processing recovery - use hash metadata to decide staleness
async function recoverProcessing(limit = 1000) {
  if (!running) return;
  const now = Date.now();
  const timeout = PROCESSING_TIMEOUT_MS;
  const items = await redis.lrange('queue:processing', 0, limit - 1);
  if (!items || items.length === 0) return;

  let recovered = 0, sentToDlq = 0;
  for (const raw of items) {
    let parsed: Task;
    try { parsed = JSON.parse(raw); } catch { continue; }
    if (!parsed?.id) continue;
    const key = `task:${parsed.id}`;
    const startedAtStr = await redis.hget(key, 'startedAt');
    if (!startedAtStr) continue;
    const startedAt = Number(startedAtStr);
    if (Number.isNaN(startedAt) || (now - startedAt) < timeout) continue;

    // try to remove the stale entry (race-safe guard)
    const removed = await redis.lrem('queue:processing', 1, raw);
    if (removed === 0) continue; // lost race

    // mark as timed out and schedule retry or DLQ
    await redis.hincrby(key, 'retries', 1);
    await redis.hset(key, 'lastError', 'processing_timeout');

    const newRetries = Number(await redis.hget(key, 'retries') ?? 0);
    const max = Number(await redis.hget(key, 'maxRetries') ?? MAX_RETRIES);

    if (newRetries <= max) {
      const nextRun = Date.now() + backoffMs(newRetries);
      const data = await redis.hget(key, 'data') || raw;
      // update the serialized task (with updated retries)
      let taskObj: Task;
      try {
        taskObj = JSON.parse(data);
      } catch { taskObj = parsed; }
      taskObj.retries = newRetries;
      taskObj.lastError = 'processing_timeout';
      taskObj.startedAt = null;
      const serialized = serializeTask(taskObj);
      await redis.zadd('queue:delayed', nextRun, serialized);
      await redis.hset(key, 'data', serialized, 'status', 'scheduled');
      recovered++;
    } else {
      const data = await redis.hget(key, 'data') || raw;
      let taskObj: Task;
      try { taskObj = JSON.parse(data); } catch { taskObj = parsed; }
      taskObj.retries = newRetries;
      taskObj.startedAt = null;
      taskObj.lastError = 'processing_timeout';
      const serialized = serializeTask(taskObj);
      await redis.lpush('queue:dead_letter', serialized);
      await redis.hset(key, 'status', 'failed', 'lastError', 'processing_timeout');
      await redis.incr('metrics:jobs_dead_letter');
      sentToDlq++;
    }
  }

  if (recovered || sentToDlq) {
    logger.warn({ recovered, sentToDlq, timeout }, 'recover_processing_results');
  }
}

setInterval(() => {
  recoverProcessing().catch(err => logger.error({ err }, 'recoverProcessing_error'));
}, PROCESSING_SCAN_INTERVAL_MS);

// graceful shutdown
async function shutdown(signal: string) {
  logger.info({ signal }, 'shutdown_initiated');
  running = false;

  // wait for loops to exit (they check running)
  const t0 = Date.now();
  const check = setInterval(async () => {
    if (activeLoops.size === 0) {
      clearInterval(check);
      try {
        await redis.quit();
      } catch { try { redis.disconnect(); } catch {} }
      server.close(() => {
        logger.info('http_closed');
        process.exit(0);
      });
    } else if ((Date.now() - t0) > 30_000) {
      // forced exit after 30s
      logger.warn('forced_exit_timeout');
      try { await redis.disconnect(); } catch {}
      process.exit(1);
    }
  }, 100);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
