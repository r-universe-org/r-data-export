import http from 'node:http';
import https from 'node:https';
import { Readable } from 'node:stream';
import createError from 'http-errors';

// The timeout includes reading the body, it is only there to catch stalled downloads
export async function openUrlStream(url, { timeout = 120000 } = {}) {
  const res = await fetch(url, { signal: AbortSignal.timeout(timeout) });
  if (!res.ok) {
    throw createError(502, `HTTP ${res.status} for: ${url}`);
  }
  return Readable.fromWeb(res.body);
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
