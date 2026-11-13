-- Run in database: integration_read. This is an integration-owned read/cache model,
-- not a direct cross-service query into the customer service's OLTP database.
CREATE TABLE IF NOT EXISTS customer_enrichment (
  customer_id uuid PRIMARY KEY, tier text NULL, region text NULL, shipping_zone text NULL,
  is_active boolean NOT NULL DEFAULT true, refreshed_at_utc timestamptz NOT NULL DEFAULT now()
);
INSERT INTO customer_enrichment(customer_id,tier,region,shipping_zone)
VALUES ('11111111-1111-1111-1111-111111111111','Gold','Tehran','TEH-CENTRAL')
ON CONFLICT(customer_id) DO NOTHING;
