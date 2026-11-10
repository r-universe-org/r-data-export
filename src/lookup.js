import http from 'node:http';
import https from 'node:https';
import { request, Agent, interceptors } from 'undici';
import createError from 'http-errors';

// No HTTP/2 (the default of fetch in node 26): all downloads share one connection, and
// when that session dies every download stalls until it times out. This blocked nearly
// all exports for hours on 2026-11-10. Same as express-frontend, see
// https://github.com/nodejs/undici/pull/5740
const dispatcher = new Agent({ allowH2: false }).compose(interceptors.redirect({ maxRedirections: 5 }));

// Rather than limiting the total time (large packages are fine as long as data keeps
// coming), give up when the server does not respond, or the body stalls, for 'timeout' ms.
// R2 normally responds in <100ms, so 15s without any data means the download is stuck.
export async function openUrlStream(url, { timeout = 15000 } = {}) {
  const { statusCode, body } = await request(url, {
    dispatcher, headersTimeout: timeout, bodyTimeout: timeout
  }).catch((err) => { throw markTimeout(err); });
  if (statusCode < 200 || statusCode >= 300) {
    body.dump().catch(() => {}); // free the connection
    throw createError(502, `HTTP ${statusCode} for: ${url}`);
  }
  // A stalled body is raised as an error on the stream, while extracting
  return body.on('error', markTimeout);
}

// Name undici timeouts TimeoutError, like AbortSignal.timeout() does, so that they
// get a 504 and are not retried (see createExporter)
function markTimeout(err) {
  if (['UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT'].includes(err.code)) {
    err.name = 'TimeoutError';
  }
  return err;
}

// Linux binaries are preferred because they are tar.gz and built most frequently
function pickBinary(binaries = []) {
  const candidates = binaries.filter(x => ['linux', 'mac'].includes(x.os) && x.fileid);
  candidates.sort((a, b) => (a.os === 'linux' ? 0 : 1) - (b.os === 'linux' ? 0 : 1));
  return candidates[0];
}

/**
 * Convert package info, as returned by /api/packages/{pkg} (i.e. get_package_info()
 * in express-frontend), to the format expected by createHandler()'s lookup.
 */
export function fromPackageInfo(info) {
  const binary = pickBinary(info._binaries);
  return {
    package: info.Package,
    lazydata: ['yes', 'true'].includes((info.LazyData || '').toLowerCase()),
    datasets: info._datasets || [],
    fileid: binary && binary.fileid
  };
}

// Unlike fetch(), node:http lets us set the Host header, which the frontend
// uses to determine the universe.
function getJson(url, headers) {
  const lib = url.startsWith('https:') ? https : http;
  return new Promise((resolve, reject) => {
    const req = lib.get(url, { headers, timeout: 30000 }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => {
        if (res.statusCode === 404) {
          return reject(createError(404, 'Package not found'));
        }
        if (res.statusCode !== 200) {
          return reject(createError(502, `HTTP ${res.statusCode} for: ${url}`));
        }
        try {
          resolve(JSON.parse(Buffer.concat(chunks)));
        } catch (err) {
          reject(createError(502, `Invalid json from: ${url}`));
        }
      });
    });
    req.on('timeout', () => req.destroy(createError(504, `Timeout for: ${url}`)));
    req.on('error', (err) => reject(err.status ? err : createError(502, `Failed to lookup package: ${err.message}`)));
  });
}

/**
 * Lookup function based on the r-universe json api. If baseUrl contains {universe}
 * it gets substituted, e.g. 'https://{universe}.r-universe.dev'. Otherwise we request
 * baseUrl with a 'Host: {universe}.r-universe.dev' header, e.g. 'http://frontend:3000'
 * to talk to the frontend container directly.
 */
export function apiLookup(baseUrl = 'https://{universe}.r-universe.dev') {
  return async function lookup(universe, pkg) {
    const path = `/api/packages/${encodeURIComponent(pkg)}`;
    const hasTemplate = baseUrl.includes('{universe}');
    const url = `${baseUrl.replace('{universe}', universe).replace(/\/$/, '')}${path}`;
    const headers = hasTemplate ? {} : { Host: `${universe}.r-universe.dev` };
    const info = await getJson(url, headers).catch((err) => {
      if (err.status === 404) err.message = `Package ${pkg} not found in ${universe}`;
      throw err;
    });
    return fromPackageInfo(info);
  };
}
