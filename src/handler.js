import createError from 'http-errors';
import { isbot } from 'isbot';
import { formats } from './formats.js';
import { openUrlStream } from './lookup.js';

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
 */
export function createHandler({ exporter, lookup, openStream = openUrlStream, blockBots = false, maxAge = 3600 }) {
  return async function handler(req, res, next) {
    const fail = (err) => {
      if (!err.status) console.log(`[R-DATA-EXPORT] ${req.url}: ${err.message}`);
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
      if (!format) {
        const path = (req.originalUrl || req.url).replace(/[?#].*/, '').replace(/\/$/, '');
        res.statusCode = 302;
        res.setHeader('Location', `${path}/csv`);
        return res.end();
      }
      if (!formats[format]) {
        throw createError(400, `Unsupported format: ${format}. Supported: ${Object.keys(formats).join(', ')}`);
      }
      const info = await lookup(universe, pkgname);
      const ds = info.datasets.find(x => x.name === name);
      if (!ds) {
        throw createError(404, `No data "${name}" found in ${pkgname}`);
      }
      if (!info.fileid) {
        throw createError(404, `No binary package available for ${pkgname}`);
      }
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
