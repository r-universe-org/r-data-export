// Child process that hosts a single webR instance, see pool.js. Everything that
// can go wrong with R (OOM, infinite loops, wasm traps) stays inside this process,
// the parent kills and replaces it when needed.
// Input and output files are exchanged via a host directory that the parent
// manages, which we mount in webR. This avoids copying large files through
// messages and keeping them in the wasm heap.
import fs from 'node:fs';
import { WebR } from '@r-universe/webr';

const config = JSON.parse(process.argv[2] || '{}');
const rcode = fs.readFileSync(new URL('./convert.R', import.meta.url), 'utf8');
let webR = null;

function send(msg) {
  if (process.connected) process.send(msg);
}

function errorMessage(err) {
  return (err && err.message || String(err)).replace(/^Error in [^:]+: /, '');
}

// The R interpreter runs in a worker thread so our main thread stays responsive
// and can watch the memory usage of the whole process (incl. the wasm heap).
function startWatchdog() {
  setInterval(() => {
    const rss = process.memoryUsage.rss();
    if (rss > config.memoryLimit) {
      send({ type: 'memory', rss });
      process.exit(70);
    }
  }, 250).unref();
}

async function init() {
  webR = new WebR({ interactive: false });
  await webR.init();
  await webR.FS.mkdir('/host');
  await webR.FS.mount('NODEFS', { root: config.dir }, '/host');
  await webR.evalRVoid(`
    datatool <- new.env(parent = baseenv())
    eval(parse(text = rcode), envir = datatool)
    assign(".datatool", datatool, envir = globalenv())
    for(pkg in c("jsonlite", "data.table", "writexl")) loadNamespace(pkg)
    data.table::setDTthreads(1)
    mem.maxVSize(vsize)
    invisible(gc())
  `, { env: { rcode, vsize: config.rVsizeMB } });
  send({ type: 'ready' });
}

async function runJob({ id, files, dataset, format }) {
  const dir = `/host/job${id}`;
  try {
    // For lazyload databases the last file is Rdata.rdx
    const input = `${dir}/${files[files.length - 1]}`;
    const output = `${dir}/output`;
    const size = await webR.evalRNumber(`.datatool$convert(input, name, format, output)`,
      { env: { input, name: dataset, format, output } });
    if (size > config.maxOutputBytes) {
      throw new Error(`Output is too large (${Math.round(size / 1e6)}MB)`);
    }
    send({ type: 'result', id, size });
  } catch (err) {
    send({ type: 'error', id, message: errorMessage(err) });
  } finally {
    // If this fails the R session is broken, let the parent replace us
    await webR.evalRVoid(`invisible(gc())`).catch(() => process.exit(71));
  }
}

process.on('message', (msg) => {
  if (msg.type === 'job') runJob(msg);
});

// Exit when the parent goes away, we should never outlive it
process.on('disconnect', () => process.exit(0));

startWatchdog();
init().catch((err) => {
  send({ type: 'fatal', message: errorMessage(err) });
  process.exit(1);
});
