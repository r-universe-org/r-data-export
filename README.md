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

Note that each process gets its own pool, so with `FRONTEND_WORKERS=4` and
`workers: 2` you could get up to 8 webR processes of ~500MB-2GB each. Workers only
start when needed and stop when idle, but if that is too much, use the standalone server.

## Standalone server

A minimal drop-in replacement for cranlike-server that needs no database. It looks
up package info from the frontend json api, and downloads the binary from R2:

```sh
PORT=3000 npx r-data-export
```

It serves `/:universe/:package/data/:name/:format`, which matches the current nginx
config (`proxy_pass http://cransrv2/$universe$request_uri`), and `/status` for monitoring.

| Environment variable        | Default                           |
|-----------------------------|-----------------------------------|
| `PORT`                      | `3000`                            |
| `UNIVERSE_API`              | `https://{universe}.r-universe.dev` |
| `DATA_EXPORT_WORKERS`       | `2`                               |
| `DATA_EXPORT_MAX_QUEUE`     | `10`                              |
| `DATA_EXPORT_JOB_TIMEOUT`   | `60000` (ms)                      |
| `DATA_EXPORT_MEMORY_LIMIT`  | `2048` (MB per worker)            |
| `PRELOAD_WEBR`              | start workers at boot             |

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
| `maxInputBytes`    | 100e6    | Max size of data files in the package (413) |
| `maxOutputBytes`   | 200e6    | Max size of the converted output (413) |
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
