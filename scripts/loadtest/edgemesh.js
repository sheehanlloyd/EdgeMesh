// k6 load test for EdgeMesh.
//
// Scenarios are selected with the SCENARIO environment variable so the same
// script covers every benchmark in docs/benchmarks.md. Each scenario isolates
// one variable; mixing them would produce numbers that cannot be attributed.
//
//   SCENARIO=l1_hit      warm cache, single edge, one key       -> L1 hit path
//   SCENARIO=l2_hit      warm cache, keys owned by other edges  -> peer path
//   SCENARIO=cold_miss   unique keys                            -> origin fill
//   SCENARIO=coalesce    many concurrent requests for one key   -> coalescing
//   SCENARIO=passthrough uncacheable responses                  -> proxy overhead
//   SCENARIO=mixed       Zipf-like key distribution             -> realistic mix

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const TARGET = __ENV.TARGET || 'http://127.0.0.1:8081';
const HOST = __ENV.HOST || 'demo.edgemesh.local';
const SCENARIO = __ENV.SCENARIO || 'mixed';
const VUS = parseInt(__ENV.VUS || '50', 10);
const DURATION = __ENV.DURATION || '30s';
const KEYSPACE = parseInt(__ENV.KEYSPACE || '1000', 10);

// Cache outcomes are tracked separately so a run reports what it actually
// exercised rather than what it intended to.
const cacheHits = new Counter('edgemesh_cache_hit');
const cachePeerHits = new Counter('edgemesh_cache_peer_hit');
const cacheMisses = new Counter('edgemesh_cache_miss');
const cacheBypass = new Counter('edgemesh_cache_bypass');
const cacheDegraded = new Counter('edgemesh_cache_degraded');
const hitRate = new Rate('edgemesh_hit_rate');
const ttfb = new Trend('edgemesh_ttfb', true);

export const options = {
  vus: VUS,
  duration: DURATION,
  // A warm-up ramp keeps connection setup out of the measured window.
  stages: __ENV.STAGES === 'off' ? undefined : [
    { duration: '5s', target: VUS },
    { duration: DURATION, target: VUS },
    { duration: '3s', target: 0 },
  ],
  thresholds: {
    // These are sanity thresholds, not published performance claims: they
    // catch a broken run, they do not certify a number.
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<5000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// zipf returns a Zipf-like index over the keyspace: a small number of keys take
// most of the traffic, which is what real cache workloads look like.
function zipf(n) {
  const r = Math.random();
  return Math.floor(n * Math.pow(r, 3));
}

function pathFor() {
  switch (SCENARIO) {
    case 'l1_hit':
      // One key, one edge: every request after the first is a local hit.
      return '/static/hot-object';
    case 'l2_hit':
      // A spread of keys, most of which are owned by another edge.
      return `/static/spread-${__ITER % KEYSPACE}`;
    case 'cold_miss':
      // Every request is a unique key, so nothing is ever cached.
      return `/static/cold-${__VU}-${__ITER}-${Date.now()}`;
    case 'coalesce':
      // One key with a slow origin: the coalescer collapses the stampede.
      return '/delay/300';
    case 'passthrough':
      // Uncacheable: measures raw proxy overhead with no cache involvement.
      return `/dynamic/${__VU}-${__ITER}`;
    case 'mixed':
    default:
      return `/static/zipf-${zipf(KEYSPACE)}`;
  }
}

export default function () {
  const res = http.get(`${TARGET}${pathFor()}`, {
    headers: { Host: HOST },
    tags: { scenario: SCENARIO },
  });

  const outcome = res.headers['X-Edgemesh-Cache'] || 'NONE';
  switch (outcome) {
    case 'HIT': cacheHits.add(1); hitRate.add(true); break;
    case 'PEER_HIT': cachePeerHits.add(1); hitRate.add(true); break;
    case 'MISS': cacheMisses.add(1); hitRate.add(false); break;
    case 'BYPASS': cacheBypass.add(1); break;
    case 'DEGRADED': cacheDegraded.add(1); hitRate.add(false); break;
  }
  ttfb.add(res.timings.waiting);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'body is not empty': (r) => r.body && r.body.length > 0,
  });
}

export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, field) =>
    m[name] && m[name].values ? m[name].values[field] : 0;

  const lines = [
    '',
    '='.repeat(72),
    `EdgeMesh load test, scenario: ${SCENARIO}`,
    '='.repeat(72),
    `  target            ${TARGET}  (Host: ${HOST})`,
    `  virtual users     ${VUS}`,
    `  duration          ${DURATION}`,
    `  keyspace          ${KEYSPACE}`,
    '',
    `  requests          ${get('http_reqs', 'count')}`,
    `  throughput        ${get('http_reqs', 'rate').toFixed(1)} req/s`,
    `  error rate        ${(get('http_req_failed', 'rate') * 100).toFixed(3)} %`,
    '',
    `  latency p50       ${get('http_req_duration', 'med').toFixed(2)} ms`,
    `  latency p90       ${get('http_req_duration', 'p(90)').toFixed(2)} ms`,
    `  latency p95       ${get('http_req_duration', 'p(95)').toFixed(2)} ms`,
    `  latency p99       ${get('http_req_duration', 'p(99)').toFixed(2)} ms`,
    `  latency max       ${get('http_req_duration', 'max').toFixed(2)} ms`,
    '',
    `  cache HIT         ${get('edgemesh_cache_hit', 'count')}`,
    `  cache PEER_HIT    ${get('edgemesh_cache_peer_hit', 'count')}`,
    `  cache MISS        ${get('edgemesh_cache_miss', 'count')}`,
    `  cache BYPASS      ${get('edgemesh_cache_bypass', 'count')}`,
    `  cache DEGRADED    ${get('edgemesh_cache_degraded', 'count')}`,
    `  hit ratio         ${(get('edgemesh_hit_rate', 'rate') * 100).toFixed(2)} %`,
    '',
    '  Record these alongside the hardware, Go version, and commit before',
    '  publishing any of them. See docs/benchmarks.md.',
    '='.repeat(72),
    '',
  ];

  return {
    stdout: lines.join('\n'),
    'loadtest-summary.json': JSON.stringify(data, null, 2),
  };
}
