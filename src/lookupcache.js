import createError from 'http-errors';

/**
 * Wrap a lookup(universe, package) function to protect the server behind it (the
 * frontend and its database) against bursts from crawlers:
 * - results are cached for ttl ms (also 404s, crawlers request many missing things)
 * - identical concurrent lookups share a single request
 * - at most maxConcurrent lookups at once, more are rejected with 503 right away
 */
export function cachedLookup(lookup, { ttl = 300000, maxEntries = 2000, maxConcurrent = 4 } = {}) {
  const cache = new Map();
  const inflight = new Map();

  function get(key) {
    const entry = cache.get(key);
    if (entry && Date.now() - entry.time < ttl) return entry;
    cache.delete(key);
  }

  function set(key, entry) {
    cache.delete(key);
    cache.set(key, entry);
    // Map keeps insertion order, so the first key is the oldest
    if (cache.size > maxEntries) cache.delete(cache.keys().next().value);
  }

  return function(universe, pkg) {
    const key = `${universe}/${pkg}`;
    const entry = get(key);
    if (entry) {
      return entry.error ? Promise.reject(entry.error) : Promise.resolve(entry.value);
    }
    if (inflight.has(key)) {
      return inflight.get(key);
    }
    if (inflight.size >= maxConcurrent) {
      const err = createError(503, 'Data export server is too busy, please try again later');
      err.headers = { 'Retry-After': '30' };
      return Promise.reject(err);
    }
    const time = Date.now();
    const promise = Promise.resolve().then(() => lookup(universe, pkg)).then((value) => {
      set(key, { time, value });
      return value;
    }, (error) => {
      if (error.status === 404) set(key, { time, error });
      throw error;
    }).finally(() => inflight.delete(key));
    inflight.set(key, promise);
    return promise;
  };
}
