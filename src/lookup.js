import { Readable } from 'node:stream';
import createError from 'http-errors';

export async function openUrlStream(url) {
  const res = await fetch(url, { signal: AbortSignal.timeout(60000) });
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

/**
 * Lookup function based on the public r-universe json api.
 * baseUrl may contain {universe}, e.g. 'https://{universe}.r-universe.dev'
 */
export function apiLookup(baseUrl = 'https://{universe}.r-universe.dev') {
  return async function lookup(universe, pkg) {
    const url = `${baseUrl.replace('{universe}', universe)}/api/packages/${encodeURIComponent(pkg)}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
    if (res.status === 404) {
      throw createError(404, `Package ${pkg} not found in ${universe}`);
    }
    if (!res.ok) {
      throw createError(502, `HTTP ${res.status} for: ${url}`);
    }
    return fromPackageInfo(await res.json());
  };
}
