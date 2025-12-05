/**
 * In-memory response cache for stream requests
 * Provides ultra-fast responses for repeated requests
 */

const cache = new Map();
const CACHE_TTL_MS = parseInt(process.env.RESPONSE_CACHE_TTL_MS) || 300000; // 5 minutes default
const MAX_CACHE_SIZE = parseInt(process.env.RESPONSE_CACHE_MAX_SIZE) || 1000;
const ENABLED = process.env.RESPONSE_CACHE_ENABLED !== 'false'; // Enabled by default

/**
 * Generate cache key from request
 */
function getCacheKey(config, type, id) {
  // Create a simple hash of the config + request
  const configStr = JSON.stringify(config);
  return `${configStr}:${type}:${id}`;
}

/**
 * Get cached response
 */
export function getCached(config, type, id) {
  if (!ENABLED) return null;

  const key = getCacheKey(config, type, id);
  const cached = cache.get(key);

  if (!cached) {
    return null;
  }

  // Check if expired
  if (Date.now() > cached.expiresAt) {
    cache.delete(key);
    return null;
  }

  console.log(`[RESPONSE CACHE] HIT for ${type}:${id} (age: ${Math.floor((Date.now() - cached.createdAt) / 1000)}s)`);
  return cached.data;
}

/**
 * Store response in cache
 */
export function setCached(config, type, id, data) {
  if (!ENABLED) return;

  // Evict oldest entries if cache is full
  if (cache.size >= MAX_CACHE_SIZE) {
    const oldestKey = cache.keys().next().value;
    cache.delete(oldestKey);
    console.log(`[RESPONSE CACHE] Evicted oldest entry (cache full at ${MAX_CACHE_SIZE})`);
  }

  const key = getCacheKey(config, type, id);
  cache.set(key, {
    data,
    createdAt: Date.now(),
    expiresAt: Date.now() + CACHE_TTL_MS
  });

  console.log(`[RESPONSE CACHE] SET for ${type}:${id} (TTL: ${CACHE_TTL_MS}ms, cache size: ${cache.size})`);
}

/**
 * Clear all cached responses
 */
export function clearCache() {
  const size = cache.size;
  cache.clear();
  console.log(`[RESPONSE CACHE] Cleared ${size} cached responses`);
}

/**
 * Get cache statistics
 */
export function getStats() {
  return {
    size: cache.size,
    maxSize: MAX_CACHE_SIZE,
    ttlMs: CACHE_TTL_MS,
    enabled: ENABLED
  };
}

// Periodic cleanup of expired entries
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;

  for (const [key, value] of cache.entries()) {
    if (now > value.expiresAt) {
      cache.delete(key);
      cleaned++;
    }
  }

  if (cleaned > 0) {
    console.log(`[RESPONSE CACHE] Cleaned up ${cleaned} expired entries`);
  }
}, 60000); // Every minute
