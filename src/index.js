import createError from 'http-errors';
import { ExportPool, withDefaults } from './pool.js';
import { extractFiles } from './extract.js';
import { formats } from './formats.js';

export { ExportPool, extractFiles, formats };
export { createHandler } from './handler.js';
export { apiLookup, fromPackageInfo, openUrlStream } from './lookup.js';

// Files to extract from the installed (binary) package for this dataset
function dataFiles(pkg, dataset, lazydata, file) {
  if (lazydata) {
    return ['Rdata.rdb', 'Rdata.rdx'];
  }
  if (file && !/\.R$/i.test(file)) {
    return [file];
  }
  throw createError(400, `Unable to export dataset "${dataset}" from ${pkg}`);
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
      const stream = await getStream();
      const buffers = await extractFiles(stream, files.map(x => `${pkg}/data/${x}`), opts.maxInputBytes);
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
    if (!inflight.has(args.key)) {
      // shared jobs should not be cancelled by a single client
      const promise = run({ ...args, signal: undefined }).finally(() => inflight.delete(args.key));
      inflight.set(args.key, promise);
    }
    return inflight.get(args.key);
  }

  return {
    exportDataset,
    convert: (args) => pool.convert(args),
    prestart: () => pool.prestart(),
    status: () => pool.status(),
    close: () => pool.close(),
    pool
  };
}
