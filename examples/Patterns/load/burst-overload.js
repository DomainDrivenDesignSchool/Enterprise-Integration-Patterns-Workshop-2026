import http from 'k6/http';
import { check } from 'k6';
export const options = { scenarios: { burst: { executor: 'constant-arrival-rate', rate: 300, timeUnit: '1s', duration: '90s', preAllocatedVUs: 100, maxVUs: 500 } } };
export default function () {
  const r = http.post(`${__ENV.BASE_URL || 'http://localhost:5000'}/orders`,
    JSON.stringify({ customerId: 'burst', total: 10, currency: 'USD' }),
    { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `${__VU}-${__ITER}` } });
  check(r, { 'not server error': x => x.status < 500 });
}
