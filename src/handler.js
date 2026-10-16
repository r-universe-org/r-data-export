import createError from 'http-errors';
import { isbot } from 'isbot';
import { formats, dataFiles } from './formats.js';
import { openUrlStream } from './lookup.js';

const tableFormats = ['csv', 'csv.gz', 'xlsx', 'ndjson'];

// Formats that should work according to the metadata from the package build.
// Datasets without this metadata are assumed to be tables.
export function availableFormats(ds) {
  return Object.keys(formats).filter(format => {
    if (tableFormats.includes(format)) return ds.table !== false;
    if (format === 'json') return ds.tojson !== false;
    return true;
  });
}

function logError(entry) {
  console.log(`[R-DATA-EXPORT] ${entry.status} ${entry.universe}/${entry.package}::${entry.dataset} (${entry.format}): ${entry.message}`);
}

function sendError(res, err) {
  const status = err.status || 500;
  res.statusCode = status;
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(err.message);
}

/**
 * Returns a (req, res, next) request handler compatible with express and plain node http.
 * Expects req.params.{universe, package, name, format}; universe may be undefined when
 * the caller knows it from elsewhere (it is only passed on to lookup).
 *
 * - exporter: object from createExporter()
 * - lookup(universe, package): resolves to {package, lazydata, datasets, fileid}
 *   where fileid identifies the binary (tar.gz) package containing the data.
 * - openStream(fileid): resolves to a readable stream of the package tarball.
 *   Defaults to downloading fileid as a url (e.g. from R2).
 * - blockBots: reject requests from crawlers and http clients like curl (see isbot).
 * - onError(entry): called for every failed request, defaults to logging a line. The entry
 *   has a 'stage' which is either 'request' (bad url, unknown or unsupported dataset)
 *   or 'export' (failed while downloading or converting, these are worth looking into).
 */
export function createHandler({ exporter, lookup, openStream = openUrlStream, blockBots = false, maxAge = 3600, onError = logError }) {
  return async function handler(req, res, next) {
    const start = Date.now();
    let stage = 'request';
    const fail = (err) => {
      const { universe, package: pkgname, name, format } = req.params;
      if (err.status !== 401) {
        onError({
          time: new Date().toISOString(), status: err.status || 500, stage, universe,
          package: pkgname, dataset: name, format, message: err.message, ms: Date.now() - start
        });
      }
      if (!err.status) console.log(err.stack);
      if (err.headers) Object.entries(err.headers).forEach(([k, v]) => res.setHeader(k, v));
      return next ? next(err) : sendError(res, err);
    };
    try {
      if (blockBots && isbot(req.headers['user-agent'])) {
        res.setHeader('Vary', 'User-Agent');
        res.setHeader('Cache-Control', 'private'); // Vary does not work in cloudflare currently
        throw createError(401, 'Bad robot! Please respect robots.txt');
      }
      const { universe, package: pkgname, name, format } = req.params;
      if (format && !formats[format]) {
        throw createError(400, `Unsupported format: ${format}. Supported: ${Object.keys(formats).join(', ')}`);
      }
      const info = await lookup(universe, pkgname);
      const ds = info.datasets.find(x => x.name === name);
      if (!ds) {
        throw createError(404, `No data "${name}" found in ${pkgname}`);
      }
      const available = availableFormats(ds);
      if (!format) {
        const path = (req.originalUrl || req.url).replace(/[?#].*/, '').replace(/\/$/, '');
        res.statusCode = 302;
        const preferred = ['csv', 'json', 'rda'].find(x => available.includes(x));
        res.setHeader('Location', `${path}/${preferred}`);
        return res.end();
      }
      // Don't bother R with conversions that already failed when the package was built
      if (!available.includes(format)) {
        const type = Array.isArray(ds.class) ? ds.class[0] : 'unknown';
        throw createError(422, `Dataset ${pkgname}::${name} of type '${type}' cannot be exported to ${format}. Available formats: ${available.join(', ')}`);
      }
      if (!info.fileid) {
        throw createError(404, `No binary package available for ${pkgname}`);
      }
      dataFiles(pkgname, name, info.lazydata, ds.file);
      stage = 'export';
      const controller = new AbortController();
      res.on('close', () => controller.abort());
      const buf = await exporter.exportDataset({
        getStream: () => openStream(info.fileid),
        package: info.package || pkgname,
        dataset: name,
        format: format,
        lazydata: info.lazydata,
        file: ds.file,
        signal: controller.signal,
        key: `${info.fileid}/${name}/${format}`
      });
      const { type, filename } = formats[format];
      res.setHeader('Content-Type', type);
      if (filename) {
        res.setHeader('Content-Disposition', `attachment; filename="${name.replace(/[^\w.-]/g, '_')}${filename}"`);
      }
      res.setHeader('Content-Length', buf.length);
      res.setHeader('Cache-Control', `public, max-age=${maxAge}`);
      res.end(buf);
    } catch (err) {
      return fail(err);
    }
  };
}
