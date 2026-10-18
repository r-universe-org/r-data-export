# @r-universe/r-data-export

Export datasets from R packages to csv, xlsx, json, ndjson, rds and rda using
[webR](https://github.com/r-universe-org/webr-bundle). This replaces the `/data/`
apis from the legacy cranlike-server, e.g. the download links on
https://tidyverse.r-universe.dev/datasets.

Datasets are read from the *binary* (installed) package, because lazydata only
exists as an `Rdata.rdb` database after installation. The module does not care
where the tarball comes from: the caller supplies a lookup function and a stream.

## Robustness

R/webR can run out of memory, hang, or crash on arbitrary package data. So:

 - Every webR instance runs in its own **child process**. If it crashes, OOMs or
   hangs, only that child dies and gets replaced. The server process is never at risk.
 - **Memory limits**: R's vector heap is capped (`mem.maxVSize`, R raises a regular
   error), and a watchdog kills the child when its total RSS (incl. the wasm heap)
   exceeds `memoryLimitMB`. Input files and output size are also capped.
 - **Timeouts**: a conversion that takes longer than `jobTimeout` is killed with SIGKILL.
 - **Bounded queue**: at most `workers` jobs run at once and `maxQueue` wait. Requests
   beyond that are rejected instantly with `503` + `Retry-After`, and so are requests
   that waited longer than `queueTimeout`. Capacity is reserved *before* downloading
   the package so we don't download stuff we can't process anyway.
 - Workers get recycled after an error, after `maxJobsPerWorker` jobs, and stop
   after `idleTimeout` of inactivity (they start on demand in ~1 sec).
 - Identical concurrent requests share a single conversion.
 - The package build already records if a dataset can be converted to a table
   (`table`) or json (`tojson`). Requests for formats that failed there are rejected
   without starting R, and `/:package/data/:name` without a format redirects to
   the first available of csv, json or rda.

Unlike the old server, all workers are identical (they preload jsonlite, data.table
and writexl) instead of having a pool per output format.

## Use in express-frontend

```js
import { createExporter, createHandler, fromPackageInfo } from '@r-universe/r-data-export';
import { get_package_info, get_stream_by_url_or_key } from '../src/db.js';

const exporter = createExporter({ workers: 2 });
const dataHandler = createHandler({
  exporter,
  lookup: (universe, pkg) => get_package_info(pkg, universe).then(fromPackageInfo),
  openStream: get_stream_by_url_or_key
});

router.get('/:package/data/:name{/:format}', function(req, res, next){
  req.params.universe = req.universe;
  return dataHandler(req, res, next);
});
```

Errors are passed to `next()` as [http-errors](https://www.npmjs.com/package/http-errors).
Pass `blockBots: true` to reject crawlers and clients like curl with a 401.

Note that each process gets its own pool, so with `FRONTEND_WORKERS=4` and
`workers: 2` you could get up to 8 webR processes of ~500MB-2GB each. Workers only
start when needed and stop when idle, but if that is too much, use the standalone server.

## Standalone server

A minimal drop-in replacement for cranlike-server that needs no database. It looks
up package info from the frontend json api, and downloads the binary from R2.
It serves `/:universe/:package/data/:name/:format`, `/status` for monitoring and
the docker healthcheck, and `/errors` (see below).

The docker image `ghcr.io/r-universe-org/r-data-export` is built on every push to
main. In docker-compose:

```yaml
  data-export:
    image: ghcr.io/r-universe-org/r-data-export:latest
    restart: always
    container_name: data-export
    mem_limit: 10g
    cpus: 4
    environment:
      UNIVERSE_API: http://frontend:3000
      DATA_EXPORT_WORKERS: 4
      DATA_EXPORT_ERROR_LOG: /app/logs/data-export-errors.jsonl
    volumes:
      - ./logs:/app/logs
```

And in nginx.conf:

```nginx
upstream datasrv {
  server data-export:3000;
  keepalive 8;
}

location ~ ^/.*/data/ {
  proxy_pass http://datasrv/$universe$request_uri;
  proxy_redirect http://datasrv/$universe/ https://$http_host/;
  proxy_redirect /$universe/ https://$http_host/;
  ...
}
```

Or run it locally with `npm start`.

| Environment variable        | Default                           |
|-----------------------------|-----------------------------------|
| `PORT`                      | `3000`                            |
| `UNIVERSE_API`              | `https://{universe}.r-universe.dev` |
| `DATA_EXPORT_WORKERS`       | `2`                               |
| `DATA_EXPORT_MAX_QUEUE`     | `10`                              |
| `DATA_EXPORT_JOB_TIMEOUT`   | `60000` (ms)                      |
| `DATA_EXPORT_MEMORY_LIMIT`  | `2048` (MB per worker)            |
| `DATA_EXPORT_ERROR_LOG`     | file to log failed requests (json lines) |
| `PRELOAD_WEBR`              | start workers at boot             |

If `UNIVERSE_API` contains `{universe}` it is substituted in the url. Otherwise
requests go to that url with a `Host: {universe}.r-universe.dev` header, so with
`http://frontend:3000` it talks to the frontend container directly instead of
going through Cloudflare.

Keep `mem_limit` above `DATA_EXPORT_WORKERS * DATA_EXPORT_MEMORY_LIMIT` (plus a
bit for the server itself), so that the per-worker watchdog kicks in before docker
kills the entire container.

### Errors

Every failed request is logged with a `stage`:

 - `request`: bad url, unknown dataset, or a format that already failed in the package
   build. These are expected, e.g. crawlers following links.
 - `export`: passed all checks but failed while downloading or converting. These are
   worth looking into.

The most recent 1000 are available from `/errors` and `/errors?stage=export`, which
is only reachable from inside the docker network:

```sh
docker exec data-export wget -qO- 'http://localhost:3000/errors?stage=export'
```

With `DATA_EXPORT_ERROR_LOG` all errors are also appended to a json lines file, so
they survive restarts. It is rotated to `.1` at 50MB. For example:

```sh
jq -r 'select(.stage == "export") | [.status, .package, .dataset, .format, .message] | @tsv' logs/data-export-errors.jsonl
```

When using the handler in your own server, pass `onError(entry)` to `createHandler()`,
for example `createErrorLog().add`.

### Caching

Successful exports and errors that will not change by retrying (400 unsupported
format, 404 unknown dataset, 413 too large, 422 cannot be converted) are sent with
`Cache-Control: public, max-age=3600`, so a CDN like Cloudflare can absorb repeated
requests, e.g. from crawlers. Temporary errors (500, 502, 503 when busy, 504) are
sent with `no-store`, which matters because caches such as nginx will store any
status that has a `max-age`.

## Options

`createExporter(options)` accepts:

| Option             | Default  | Description |
|--------------------|----------|-------------|
| `workers`          | 2        | Number of webR processes (~500MB each when idle) |
| `maxQueue`         | 10       | Max jobs waiting for a worker, more are rejected with 503 |
| `queueTimeout`     | 30000    | Reject jobs (503) that waited this long for a worker |
| `jobTimeout`       | 60000    | Kill the worker when a conversion takes longer (504) |
| `startTimeout`     | 30000    | Kill the worker when webR does not start in time |
| `memoryLimitMB`    | 2048     | Kill the worker when its RSS exceeds this (413) |
| `rVsizeMB`         | 1000     | R vector heap limit, see `?mem.maxVSize` (413) |
| `maxJobsPerWorker` | 100      | Recycle workers after this many jobs |
| `idleTimeout`      | 600000   | Stop idle workers after this long (0 = never) |
| `maxInputBytes`    | 250e6    | Max size of data files in the package (413) |
| `maxOutputBytes`   | 200e6    | Max size of the converted output (413) |
| `failureTTL`       | 86400000 | Remember conversions that failed because of the data (R errors, limits, timeouts) |
| `log`              | console  | Function for log messages |

The exporter object has:

 - `exportDataset({getStream, package, dataset, format, lazydata, file, signal, key})`:
   extract the data from a package tarball and convert it. Resolves to a Buffer.
 - `convert({files, dataset, format, signal})`: convert data files you already have.
 - `status()`, `prestart()`, `close()`

## Tests

```sh
npm test
```

Fixtures in `test/fixtures` are created with `Rscript test/make-fixtures.R`.
