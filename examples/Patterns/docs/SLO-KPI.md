# Business KPIs, SLIs and SLAs

Example workshop objectives (targets are illustrative and must be agreed with the business):
- KPI: accepted orders completed within 5 minutes.
- SLI: percentage of eligible orders completed within 5 minutes.
- SLO: 99% over rolling 30 days.
- Payment success ratio, inventory reservation success, DLQ age, order end-to-end p95.
- Technical SLIs: API p95/p99, consumer lag age, event publish delay, retry ratio, error rate.

Do not define availability solely as HTTP 200. An order accepted but never fulfilled is a business failure.
Use histogram buckets and percentiles from histograms; do not average p99 values across instances.
