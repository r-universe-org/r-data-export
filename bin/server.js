#!/usr/bin/env node
// Standalone drop-in replacement for the cranlike-server /data/ apis. Nginx proxies
// /:universe/:package/data/... here, package info is looked up via the frontend json api.
import http from 'node:http';
import { createExporter, createHandler, apiLookup } from '../src/index.js';

const env = process.env;
const num = (x) => x === undefined ? undefined : Number(x);
const port = num(env.PORT) || 3000;
const exporter = createExporter({
  workers: num(env.DATA_EXPORT_WORKERS),
  maxQueue: num(env.DATA_EXPORT_MAX_QUEUE),
  jobTimeout: num(env.DATA_EXPORT_JOB_TIMEOUT),
  memoryLimitMB: num(env.DATA_EXPORT_MEMORY_LIMIT)
});
const lookup = apiLookup(env.UNIVERSE_API || 'https://{universe}.r-universe.dev');
const handler = createHandler({ exporter, lookup });

const server = http.createServer(function(req, res) {
  const path = req.url.replace(/[?#].*/, '');
  if (path === '/status') {
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify(exporter.status()));
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.statusCode = 405;
    return res.end();
  }
  const m = path.match(/^\/([\w.-]+)\/([\w.]+)\/data\/([^/]+)(?:\/([\w.]+))?\/?$/);
  if (!m) {
    res.statusCode = 404;
    return res.end('Not found');
  }
  req.params = { universe: m[1], package: m[2], name: decodeURIComponent(m[3]), format: m[4] };
  handler(req, res);
});

if (env.PRELOAD_WEBR) exporter.prestart();
server.listen(port, () => console.log(`Data export server listening on port ${port}`));

function shutdown() {
  console.log('Shutting down data export server');
  exporter.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
