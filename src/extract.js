import { createGunzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import tar from 'tar-stream';
import createError from 'http-errors';

function stream2buffer(stream, maxBytes, filename) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    stream.on('data', (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        stream.destroy(createError(413, `File ${filename} is too large`));
      } else {
        chunks.push(chunk);
      }
    });
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}

// Extract the given files from a .tar.gz stream. Stops reading as soon as all
// files are found. Fails when a file is missing or exceeds maxBytes.
export function extractFiles(input, files, maxBytes = 100e6) {
  const output = Array(files.length);
  const remaining = new Set(files);
  const controller = new AbortController();
  let failure = null;
  function processEntry(header, filestream, next) {
    filestream.on('end', next);
    filestream.on('error', () => {});
    const index = files.indexOf(header.name);
    if (index < 0) {
      return filestream.resume();
    }
    if (header.size > maxBytes) {
      failure = createError(413, `File ${header.name} is too large (${Math.round(header.size / 1e6)}MB)`);
      return controller.abort();
    }
    stream2buffer(filestream, maxBytes, header.name).then((buf) => {
      output[index] = buf;
      remaining.delete(header.name);
      if (!remaining.size) controller.abort();
    }, (err) => {
      failure = err;
      controller.abort();
    });
  }
  const extract = tar.extract({ allowUnknownFormat: true }).on('entry', processEntry);
  return pipeline(input, createGunzip({ chunkSize: 64 * 1024 }), extract, { signal: controller.signal }).catch((err) => {
    // When we stop reading early, the source may raise its own error, e.g. an http
    // response reports 'aborted'. Only errors that happened before that are real.
    if (!controller.signal.aborted) throw err;
  }).then(() => {
    if (failure) throw failure;
    const missing = files.filter((x, i) => !output[i] || !output[i].length);
    if (missing.length) throw createError(404, `Failed to find ${missing.join(', ')} in package`);
    return output;
  });
}
