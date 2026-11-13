import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

export const errors = new Rate('order_errors');
export const latency = new Trend('order_latency', true);
export const options = {
  scenarios: { steady: { executor: 'ramping-arrival-rate', startRate: 20, timeUnit: '1s',
    preAllocatedVUs: 20, maxVUs: 200, stages: [{duration:'1m',target:50},{duration:'3m',target:100},{duration:'1m',target:0}] } },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500','p(99)<1000'], order_errors: ['rate<0.01'] }
};
export default function () {
  const payload = JSON.stringify({ customerId: `customer-${__VU}`, total: 125.50, currency: 'USD' });
  const res = http.post(`${__ENV.BASE_URL || 'http://localhost:5000'}/orders`, payload,
    { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `${__VU}-${__ITER}` } });
  latency.add(res.timings.duration);
  errors.add(res.status >= 400);
  check(res, { 'accepted': r => r.status === 202 || r.status === 201 });
  sleep(0.1);
}
