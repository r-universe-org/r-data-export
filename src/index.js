import createError from 'http-errors';
import { ExportPool, withDefaults } from './pool.js';
import { extractFiles } from './extract.js';
import { formats, dataFiles } from './formats.js';

export { ExportPool, extractFiles, formats };
export { createHandler, availableFormats } from './handler.js';
export { apiLookup, fromPackageInfo, openUrlStream } from './lookup.js';
export { createErrorLog } from './errorlog.js';
export { cachedLookup } from './lookupcache.js';

// undici (fetch) puts the actual reason in err.cause, e.g. 'terminated' (other side closed)
function errorMessage(err) {
  const cause = err.cause && (err.cause.message || err.cause.code);
  return cause ? `${err.message} (${cause})` : err.message;
}

/**
 * Create a data exporter backed by a pool of webR child processes.
 * See pool.js for all options.
 */
export function createExporter(options = {}) {
  const opts = withDefaults(options);
  const pool = new ExportPool(opts);
  const inflight = new Map();

  async function run({ getStream, package: pkg, dataset, format, lazydata, file, signal }) {
    if (!formats[format]) {
      throw createError(400, `Unsupported format: ${format}`);
    }
    const files = dataFiles(pkg, dataset, lazydata, file);
    const release = pool.admit();
    try {
      const download = () => Promise.resolve().then(getStream).then((stream) => {
        return extractFiles(stream, files.map(x => `${pkg}/data/${x}`), opts.maxInputBytes);
      });
      // A broken connection is worth one more try: with HTTP/2 (node 26) all downloads
      // share one connection, and when it gets closed they all fail with 'terminated'.
      // Not for http errors (e.g. 404, too large) or timeouts.
      const buffers = await download().catch((err) => {
        if (err.status || err.name === 'TimeoutError') throw err;
        opts.log(`retrying download of ${pkg} after: ${errorMessage(err)}`);
        return download();
      }).catch((err) => {
        if (err.status) throw err;
        const status = err.name === 'TimeoutError' ? 504 : 502;
        throw createError(status, `Failed to download ${pkg} package: ${errorMessage(err)}`);
      });
      signal?.throwIfAborted();
      return await pool.enqueue({
        files: files.map((name, i) => ({ name, data: new Uint8Array(buffers[i]) })),
        dataset, format, label: `${pkg}::${dataset} to ${format}`, signal
      });
    } finally {
      release();
    }
  }

  /**
   * Export a dataset from a binary package to the given format. Resolves to a Buffer.
   * - getStream: function returning (a promise of) a readable stream of the package .tar.gz
   * - key: optional unique key (e.g. file hash + dataset + format) to share the
   *   result between identical concurrent requests
   */
  function exportDataset(args) {
    if (!args.key) return run(args);
    const failure = failures.get(args.key);
    if (failure && Date.now() - failure.time < opts.failureTTL) {
      return Promise.reject(failure.error);
    }
    if (!inflight.has(args.key)) {
      // shared jobs should not be cancelled by a single client
      const promise = run({ ...args, signal: undefined }).catch((err) => {
        if (isDeterministic(err)) rememberFailure(args.key, err);
        throw err;
      }).finally(() => inflight.delete(args.key));
      inflight.set(args.key, promise);
    }
    return inflight.get(args.key);
  }

  // Conversions that failed because of the data itself (R errors, size or memory
  // limits, timeouts) will fail again, so remember those. Not busy (503) or download
  // errors, which are temporary. The key includes the file id of the package, so a
  // new build of the package gets a new try.
  const failures = new Map();
  function isDeterministic(err) {
    return err.status === 413 || err.status === 422 || (err.status === 504 && /^Failed to convert/.test(err.message));
  }
  function rememberFailure(key, error) {
    failures.delete(key);
    failures.set(key, { time: Date.now(), error });
    if (failures.size > 10000) failures.delete(failures.keys().next().value);
  }

  return {
    exportDataset,
    convert: (args) => pool.convert(args),
    isFull: () => pool.isFull(),
    prestart: () => pool.prestart(),
    status: () => pool.status(),
    close: () => pool.close(),
    pool
  };
}
