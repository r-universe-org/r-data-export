import { fork } from 'node:child_process';
import createError from 'http-errors';

const childPath = new URL('./child.js', import.meta.url);

export const defaults = {
  workers: 2,              // number of webR processes (each uses ~500MB idle)
  maxQueue: 10,            // reject new jobs instantly when this many are waiting
  queueTimeout: 30000,     // reject jobs that waited this long for a worker
  jobTimeout: 60000,       // kill the worker when a conversion takes longer
  startTimeout: 30000,     // kill the worker when webR does not start in time
  memoryLimitMB: 2048,     // kill the worker when its rss exceeds this
  rVsizeMB: 1000,          // R vector heap limit, R errors when exceeded (see ?mem.maxVSize)
  maxJobsPerWorker: 100,   // recycle workers after this many jobs
  idleTimeout: 600000,     // stop idle workers after this long (0 = never)
  maxInputBytes: 100e6,    // fail when data files in the package are larger
  maxOutputBytes: 200e6,   // fail conversions that produce larger output
  log: (msg) => console.log(`[R-DATA-EXPORT] ${msg}`)
};

// Idle workers should not keep the parent process alive
function setRef(child, ref) {
  if (ref) {
    child.ref();
    child.channel?.ref();
  } else {
    child.unref();
    child.channel?.unref();
  }
}

// Merge options with defaults, ignoring undefined values
export function withDefaults(options) {
  const defined = Object.entries(options).filter(([, v]) => v !== undefined);
  return { ...defaults, ...Object.fromEntries(defined) };
}

class Worker {
  constructor(pool) {
    this.pool = pool;
    this.state = 'starting';
    this.jobs = 0;
    this.job = null;
    this.exitReason = null;
    const { options } = pool;
    const config = {
      memoryLimit: options.memoryLimitMB * 1024 * 1024,
      rVsizeMB: options.rVsizeMB,
      maxOutputBytes: options.maxOutputBytes
    };
    // Advanced serialization passes Uint8Arrays without converting to json.
    // Don't inherit the parent's node flags, e.g. --input-type breaks the child.
    this.child = fork(childPath, [JSON.stringify(config)], {
      serialization: 'advanced',
      execArgv: ['--max-old-space-size=512'],
      stdio: ['ignore', 'inherit', 'inherit', 'ipc']
    });
    this.child.on('message', (msg) => this.onMessage(msg));
    this.child.on('exit', (code, signal) => this.onExit(code, signal));
    this.child.on('error', (err) => options.log(`worker error: ${err.message}`));
    this.timer = setTimeout(() => this.kill('startup timeout'), options.startTimeout);
  }

  onMessage(msg) {
    const job = this.job;
    switch (msg.type) {
      case 'ready':
        this.pool.startFailures = 0;
        this.setIdle();
        this.pool.dispatch();
        break;
      case 'result':
      case 'error':
        if (!job || job.id !== msg.id) return;
        this.finishJob();
        if (msg.type === 'result') {
          job.resolve(Buffer.from(msg.data.buffer, msg.data.byteOffset, msg.data.byteLength));
        } else {
          // Errors are usually caused by the data, but a fresh R session is cheap insurance
          const status = /too large|memory|cannot allocate/i.test(msg.message) ? 413 : 422;
          job.reject(createError(status, `Failed to convert ${job.label}: ${msg.message}`));
          this.retire('conversion error');
        }
        break;
      case 'memory':
        this.exitReason = `memory limit exceeded (${Math.round(msg.rss / 1e6)}MB)`;
        break;
      case 'fatal':
        this.exitReason = `failed to start: ${msg.message}`;
        break;
    }
  }

  onExit(code, signal) {
    clearTimeout(this.timer);
    const reason = this.exitReason || `exited (code ${code}, signal ${signal})`;
    const wasStarting = this.state === 'starting';
    this.state = 'dead';
    this.pool.remove(this);
    if (this.job) {
      const job = this.job;
      this.job = null;
      this.pool.options.log(`worker ${reason} while converting ${job.label}`);
      const status = reason.startsWith('memory') ? 413 : reason === 'timeout' ? 504 : 500;
      job.reject(createError(status, `Failed to convert ${job.label}: ${reason}`));
    } else if (wasStarting) {
      this.pool.options.log(`worker ${reason}`);
      this.pool.startFailed();
    }
    this.pool.dispatch();
  }

  run(job) {
    this.state = 'busy';
    this.job = job;
    this.jobs++;
    clearTimeout(this.timer);
    setRef(this.child, true);
    this.timer = setTimeout(() => this.kill('timeout'), this.pool.options.jobTimeout);
    this.child.send({ type: 'job', id: job.id, files: job.files, dataset: job.dataset, format: job.format });
  }

  finishJob() {
    this.job = null;
    if (this.jobs >= this.pool.options.maxJobsPerWorker) {
      this.retire('max jobs reached');
    } else {
      this.setIdle();
    }
    setImmediate(() => this.pool.dispatch());
  }

  setIdle() {
    clearTimeout(this.timer);
    this.state = 'idle';
    setRef(this.child, false);
    if (this.pool.options.idleTimeout > 0) {
      this.timer = setTimeout(() => this.retire('idle'), this.pool.options.idleTimeout);
      this.timer.unref();
    }
  }

  // Gracefully replace this worker
  retire(reason) {
    if (this.state === 'dead') return;
    this.exitReason = reason;
    this.state = 'dead';
    this.pool.remove(this);
    this.child.kill('SIGKILL');
    this.pool.dispatch();
  }

  kill(reason) {
    this.exitReason = reason;
    this.child.kill('SIGKILL');
  }
}

export class ExportPool {
  constructor(options = {}) {
    this.options = withDefaults(options);
    this.workers = new Set();
    this.queue = [];
    this.pending = 0; // admitted jobs that are not finished, see admit()
    this.counter = 0;
    this.startFailures = 0;
    this.restartTimer = null;
    this.closed = false;
    this.stats = { completed: 0, failed: 0, rejected: 0, timedout: 0 };
  }

  // Reserve a spot for a job before doing expensive preparation (downloading).
  // Admitted jobs count against the capacity until released, whether they are
  // preparing, queued or running. Throws 503 when the server is too busy.
  admit() {
    if (this.closed || this.pending >= this.options.workers + this.options.maxQueue) {
      this.stats.rejected++;
      const err = createError(503, 'Data export server is too busy, please try again later');
      err.headers = { 'Retry-After': '30' };
      throw err;
    }
    this.pending++;
    let released = false;
    return () => {
      if (!released) this.pending--;
      released = true;
    };
  }

  // Convert dataset 'dataset' from the given input files to 'format'.
  // files: [{name, data}] where name is the basename and data a Uint8Array.
  async convert({ files, dataset, format, label, signal }) {
    const release = this.admit();
    try {
      return await this.enqueue({ files, dataset, format, label, signal });
    } finally {
      release();
    }
  }

  // Like convert() but for callers that already called admit()
  enqueue({ files, dataset, format, label, signal }) {
    if (this.closed) return Promise.reject(createError(503, 'Data export server is shutting down'));
    return new Promise((resolve, reject) => {
      const job = { id: ++this.counter, files, dataset, format, label: label || dataset, signal };
      job.resolve = (x) => { this.stats.completed++; cleanup(); resolve(x); };
      job.reject = (e) => { this.stats.failed++; cleanup(); reject(e); };
      const onabort = () => {
        if (this.dequeue(job)) {
          cleanup();
          reject(createError(499, 'Request aborted'));
        }
      };
      const cleanup = () => {
        clearTimeout(job.queueTimer);
        signal?.removeEventListener('abort', onabort);
      };
      job.queueTimer = setTimeout(() => {
        if (this.dequeue(job)) {
          this.stats.timedout++;
          cleanup();
          const err = createError(503, 'Timeout waiting for available data export worker');
          err.headers = { 'Retry-After': '30' };
          reject(err);
        }
      }, this.options.queueTimeout);
      signal?.addEventListener('abort', onabort);
      this.queue.push(job);
      this.dispatch();
    });
  }

  dequeue(job) {
    const index = this.queue.indexOf(job);
    if (index < 0) return false;
    this.queue.splice(index, 1);
    return true;
  }

  dispatch() {
    while (this.queue.length) {
      const idle = [...this.workers].find(w => w.state === 'idle');
      if (idle) {
        idle.run(this.queue.shift());
        continue;
      }
      if (this.workers.size < this.options.workers && !this.restartTimer && !this.closed) {
        this.workers.add(new Worker(this));
      }
      break;
    }
  }

  remove(worker) {
    this.workers.delete(worker);
  }

  // Back off when workers fail to start (e.g. broken installation) so we don't spin
  startFailed() {
    this.startFailures++;
    const delay = Math.min(30000, 1000 * 2 ** (this.startFailures - 1));
    clearTimeout(this.restartTimer);
    this.restartTimer = setTimeout(() => {
      this.restartTimer = null;
      this.dispatch();
    }, delay);
    this.restartTimer.unref();
  }

  // Start all workers ahead of time so the first requests are fast
  prestart() {
    while (this.workers.size < this.options.workers) {
      this.workers.add(new Worker(this));
    }
  }

  status() {
    const workers = [...this.workers].map(w => ({ pid: w.child.pid, state: w.state, jobs: w.jobs }));
    return { workers, admitted: this.pending, queued: this.queue.length, ...this.stats };
  }

  close() {
    this.closed = true;
    clearTimeout(this.restartTimer);
    for (const job of this.queue.splice(0)) {
      job.reject(createError(503, 'Data export server is shutting down'));
    }
    for (const worker of this.workers) {
      worker.retire('shutdown');
    }
  }
}
