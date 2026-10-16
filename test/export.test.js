import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import { createGzip } from 'node:zlib';
import tar from 'tar-stream';
import os from 'node:os';
import path from 'node:path';
import { createExporter, createHandler, createErrorLog, extractFiles } from '../src/index.js';

const fixture = (name) => fs.readFileSync(new URL(`./fixtures/${name}`, import.meta.url));
const quiet = { log: () => {} };

// Fake installed package tarball with the given files in pkg/data/
function tarball(pkg, files) {
  const pack = tar.pack();
  pack.entry({ name: `${pkg}/DESCRIPTION` }, 'Package: test\n');
  for (const name of files) {
    pack.entry({ name: `${pkg}/data/${name}` }, fixture(name));
  }
  pack.finalize();
  return pack.pipe(createGzip());
}

function input(name) {
  return [{ name, data: new Uint8Array(fixture(name)) }];
}

test('export lazydata and rda files to all formats', async () => {
  const exporter = createExporter({ workers: 1, ...quiet });
  try {
    const csv = await exporter.exportDataset({
      getStream: () => tarball('testpkg', ['Rdata.rdb', 'Rdata.rdx']),
      package: 'testpkg', dataset: 'cars2', format: 'csv', lazydata: true
    });
    assert.match(csv.toString(), /^speed,dist\n4,2\n/);
    const json = await exporter.exportDataset({
      getStream: () => tarball('testpkg', ['small.rda']),
      package: 'testpkg', dataset: 'small', format: 'json', file: 'small.rda'
    });
    assert.equal(JSON.parse(json).length, 10);
    for (const format of ['xlsx', 'ndjson', 'rds', 'rda', 'csv.gz']) {
      const out = await exporter.convert({ files: input('small.rda'), dataset: 'small', format });
      assert.ok(out.length > 50, format);
    }
    assert.equal(exporter.status().workers.length, 1);
  } finally {
    exporter.close();
  }
});

test('R errors are reported and the worker is kept', async () => {
  const exporter = createExporter({ workers: 1, ...quiet });
  try {
    await assert.rejects(exporter.convert({ files: input('notdf.rda'), dataset: 'notdf', format: 'csv' }),
      { status: 422, message: /Failed to convert notdf/ });
    const pid = exporter.status().workers[0].pid;
    await assert.rejects(exporter.convert({ files: input('small.rda'), dataset: 'wrong', format: 'csv' }),
      { status: 422, message: /not found/ });
    const out = await exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' });
    assert.match(out.toString(), /^Sepal.Length/);
    assert.equal(exporter.status().workers[0].pid, pid);
    assert.equal(exporter.status().workers[0].jobs, 3);
  } finally {
    exporter.close();
  }
});

test('large input files and temp dir cleanup', async () => {
  const exporter = createExporter({ workers: 1, jobTimeout: 500, ...quiet });
  const tempdirs = () => fs.readdirSync(os.tmpdir()).filter(x => x.startsWith('r-data-export-'));
  const before = tempdirs();
  try {
    // webR's FS.writeFile used to fail with 'Invalid array length' for files over ~130MB
    const files = [{ name: 'padding.bin', data: new Uint8Array(150e6) }, ...input('small.rda')];
    const out = await exporter.convert({ files, dataset: 'small', format: 'csv' });
    assert.match(out.toString(), /^Sepal.Length/);
    const workerdir = tempdirs().filter(x => !before.includes(x));
    assert.equal(workerdir.length, 1);
    assert.deepEqual(fs.readdirSync(path.join(os.tmpdir(), workerdir[0])), []);
    // killed workers also get cleaned up
    await assert.rejects(exporter.convert({ files: input('big.rda'), dataset: 'big', format: 'json' }), { status: 504 });
    await new Promise(resolve => setTimeout(resolve, 200));
    assert.deepEqual(tempdirs().filter(x => !before.includes(x)), []);
    // and so do workers that are running when we close
    await exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' });
    assert.equal(tempdirs().filter(x => !before.includes(x)).length, 1);
    exporter.close();
    assert.deepEqual(tempdirs().filter(x => !before.includes(x)), []);
  } finally {
    exporter.close();
  }
});

test('R vector memory limit, the worker is replaced', async () => {
  const exporter = createExporter({ workers: 1, rVsizeMB: 100, ...quiet });
  try {
    await assert.rejects(exporter.convert({ files: input('big.rda'), dataset: 'big', format: 'csv' }),
      { status: 413, message: /memory/ });
    assert.equal(exporter.status().workers.length, 0);
    const out = await exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' });
    assert.match(out.toString(), /^Sepal.Length/);
  } finally {
    exporter.close();
  }
});

test('process memory watchdog kills the worker', async () => {
  const exporter = createExporter({ workers: 1, rVsizeMB: 8000, memoryLimitMB: 750, ...quiet });
  try {
    await assert.rejects(exporter.convert({ files: input('big.rda'), dataset: 'big', format: 'json' }),
      { status: 413, message: /memory limit exceeded/ });
    const out = await exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' });
    assert.match(out.toString(), /^Sepal.Length/);
  } finally {
    exporter.close();
  }
});

test('job timeout kills the worker, next job gets a fresh one', async () => {
  const exporter = createExporter({ workers: 1, jobTimeout: 500, ...quiet });
  try {
    await assert.rejects(exporter.convert({ files: input('big.rda'), dataset: 'big', format: 'json' }),
      { status: 504, message: /timeout/ });
    const out = await exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' });
    assert.match(out.toString(), /^Sepal.Length/);
  } finally {
    exporter.close();
  }
});

test('reject instantly when queue is full', async () => {
  const exporter = createExporter({ workers: 1, maxQueue: 1, ...quiet });
  try {
    const job = () => exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' });
    const jobs = [job(), job()];
    const start = Date.now();
    await assert.rejects(job(), { status: 503, headers: { 'Retry-After': '30' } });
    assert.ok(Date.now() - start < 50);
    await Promise.all(jobs);
    assert.equal(exporter.status().rejected, 1);
  } finally {
    exporter.close();
  }
});

test('queue timeout and abort signal', async () => {
  const exporter = createExporter({ workers: 1, queueTimeout: 50, ...quiet });
  try {
    // Cold start takes longer than 50ms
    await assert.rejects(exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' }),
      { status: 503, message: /Timeout waiting/ });
    const controller = new AbortController();
    const promise = exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv', signal: controller.signal });
    controller.abort();
    await assert.rejects(promise, { status: 499 });
  } finally {
    exporter.close();
  }
});

test('workers that fail to start do not hang the queue', async () => {
  const exporter = createExporter({ workers: 1, memoryLimitMB: 50, queueTimeout: 3000, ...quiet });
  try {
    await assert.rejects(exporter.convert({ files: input('small.rda'), dataset: 'small', format: 'csv' }),
      { status: 503 });
  } finally {
    exporter.close();
  }
});

test('identical concurrent requests share a single conversion', async () => {
  const exporter = createExporter({ workers: 1, ...quiet });
  let downloads = 0;
  const args = {
    getStream: () => { downloads++; return tarball('testpkg', ['small.rda']); },
    package: 'testpkg', dataset: 'small', format: 'csv', file: 'small.rda', key: 'abc/small/csv'
  };
  try {
    const [a, b] = await Promise.all([exporter.exportDataset(args), exporter.exportDataset(args)]);
    assert.equal(downloads, 1);
    assert.deepEqual(a, b);
  } finally {
    exporter.close();
  }
});

test('download errors are reported as 502/504', async () => {
  const exporter = createExporter({ workers: 1, ...quiet });
  const args = { package: 'testpkg', dataset: 'small', format: 'csv', file: 'small.rda' };
  try {
    await assert.rejects(exporter.exportDataset({ ...args, getStream: () => { throw new Error('connection reset'); } }),
      { status: 502, message: /Failed to download testpkg package: connection reset/ });
    const timeout = Object.assign(new Error('The operation was aborted due to timeout'), { name: 'TimeoutError' });
    await assert.rejects(exporter.exportDataset({ ...args, getStream: async () => { throw timeout; } }),
      { status: 504 });
    assert.equal(exporter.status().admitted, 0);
  } finally {
    exporter.close();
  }
});

test('extractFiles limits', async () => {
  await assert.rejects(extractFiles(tarball('testpkg', ['small.rda']), ['testpkg/data/other.rda']),
    { status: 404 });
  await assert.rejects(extractFiles(tarball('testpkg', ['big.rda']), ['testpkg/data/big.rda'], 1000),
    { status: 413 });
});

test('http handler', async () => {
  const exporter = createExporter({ workers: 1, ...quiet });
  const errors = [];
  const handler = createHandler({
    exporter,
    lookup: async (universe, pkg) => ({
      package: pkg, lazydata: true, fileid: 'abc', datasets: [
        { name: 'cars2' },
        { name: 'list2', class: ['list'], table: false, tojson: true },
        { name: 'model2', class: ['lm'], table: false, tojson: false },
        { name: 'missing', class: ['data.frame'], table: true, tojson: true }
      ]
    }),
    openStream: async () => tarball('testpkg', ['Rdata.rdb', 'Rdata.rdx']),
    onError: (entry) => errors.push(entry)
  });
  const server = http.createServer((req, res) => {
    const [, pkg, , name, format] = req.url.split('/');
    req.params = { universe: 'test', package: pkg, name, format };
    handler(req, res);
  });
  await new Promise(resolve => server.listen(0, resolve));
  const base = `http://localhost:${server.address().port}`;
  const headers = { 'user-agent': 'Mozilla/5.0 Firefox/130' };
  try {
    let res = await fetch(`${base}/testpkg/data/cars2/xlsx`, { headers });
    assert.equal(res.status, 200);
    assert.equal(res.headers.get('content-disposition'), 'attachment; filename="cars2.xlsx"');
    assert.equal(res.headers.get('cache-control'), 'public, max-age=3600');
    res = await fetch(`${base}/testpkg/data/cars2/csv`, { headers: { 'user-agent': 'curl/8.7.1' } });
    assert.equal(res.status, 200);

    // Redirect to a format that works for this dataset
    for (const [name, format] of [['cars2', 'csv'], ['list2', 'json'], ['model2', 'rda']]) {
      res = await fetch(`${base}/testpkg/data/${name}`, { headers, redirect: 'manual' });
      assert.equal(res.status, 302);
      assert.equal(res.headers.get('location'), `/testpkg/data/${name}/${format}`);
    }

    // Formats that failed in the package build are rejected without running R
    res = await fetch(`${base}/testpkg/data/list2/csv`, { headers });
    assert.equal(res.status, 422);
    assert.match(await res.text(), /type 'list' cannot be exported to csv. Available formats: json, rds, rda/);
    res = await fetch(`${base}/testpkg/data/model2/json`, { headers });
    assert.equal(res.status, 422);

    res = await fetch(`${base}/testpkg/data/nope/csv`, { headers });
    assert.equal(res.status, 404);
    res = await fetch(`${base}/testpkg/data/cars2/exe`, { headers });
    assert.equal(res.status, 400);

    // Passes the checks but fails in R
    res = await fetch(`${base}/testpkg/data/missing/csv`, { headers });
    assert.equal(res.status, 422);

    assert.deepEqual(errors.map(x => [x.stage, x.status, x.dataset, x.format]), [
      ['request', 422, 'list2', 'csv'],
      ['request', 422, 'model2', 'json'],
      ['request', 404, 'nope', 'csv'],
      ['request', 400, 'cars2', 'exe'],
      ['export', 422, 'missing', 'csv']
    ]);
    assert.equal(errors[4].universe, 'test');
    assert.match(errors[4].message, /Dataset 'missing' not found/);
    assert.equal(exporter.status().completed, 2);
  } finally {
    server.close();
    exporter.close();
  }
});

test('error log keeps recent errors and writes rotated json lines', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'errorlog-'));
  const file = path.join(dir, 'errors.jsonl');
  const log = console.log;
  console.log = () => {};
  try {
    const errors = createErrorLog({ file, maxEntries: 3, maxFileBytes: 700 });
    for (let i = 0; i < 5; i++) {
      errors.add({ status: 422, stage: i % 2 ? 'export' : 'request', universe: 'u', package: 'p', dataset: `d${i}`, format: 'csv', message: 'x'.repeat(100) });
    }
    const all = errors.summary();
    assert.deepEqual(all.counts, { 'request 422': 3, 'export 422': 2 });
    assert.deepEqual(all.recent.map(x => x.dataset), ['d4', 'd3', 'd2']);
    assert.deepEqual(errors.summary('export').recent.map(x => x.dataset), ['d3']);
    const lines = (f) => fs.readFileSync(f, 'utf8').trim().split('\n').map(x => JSON.parse(x).dataset);
    assert.deepEqual([...lines(`${file}.1`), ...lines(file)], ['d0', 'd1', 'd2', 'd3', 'd4']);
  } finally {
    console.log = log;
    fs.rmSync(dir, { recursive: true });
  }
});

test('xlsx truncates strings longer than the excel limit', async () => {
  const exporter = createExporter({ workers: 1, ...quiet });
  try {
    const out = await exporter.convert({ files: input('longstr.rda'), dataset: 'longstr', format: 'xlsx' });
    assert.equal(out.subarray(0, 2).toString(), 'PK');
    const csv = await exporter.convert({ files: input('longstr.rda'), dataset: 'longstr', format: 'csv' });
    assert.ok(csv.length > 40000, 'csv keeps the full string');
  } finally {
    exporter.close();
  }
});

test('datasets from R scripts are rejected before exporting', async () => {
  const errors = [];
  const handler = createHandler({
    exporter: null,
    lookup: async () => ({ package: 'testpkg', lazydata: false, fileid: 'abc', datasets: [{ name: 'gen', file: 'gen.R' }] }),
    onError: (entry) => errors.push(entry)
  });
  const server = http.createServer((req, res) => {
    req.params = { universe: 'test', package: 'testpkg', name: 'gen', format: 'csv' };
    handler(req, res);
  });
  await new Promise(resolve => server.listen(0, resolve));
  try {
    const res = await fetch(`http://localhost:${server.address().port}/`);
    assert.equal(res.status, 422);
    assert.match(await res.text(), /generated by the R script gen.R/);
    assert.equal(errors[0].stage, 'request');
  } finally {
    server.close();
  }
});

test('optionally block bots', async () => {
  const handler = createHandler({ exporter: null, lookup: null, blockBots: true });
  const server = http.createServer((req, res) => {
    req.params = { universe: 'test', package: 'testpkg', name: 'cars2', format: 'csv' };
    handler(req, res);
  });
  await new Promise(resolve => server.listen(0, resolve));
  try {
    const res = await fetch(`http://localhost:${server.address().port}/`, { headers: { 'user-agent': 'curl/8.7.1' } });
    assert.equal(res.status, 401);
  } finally {
    server.close();
  }
});
