-- Metabase native question export
-- card_id: 130
-- card_name: Свежесть данных
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Яндекс Маркет`

SELECT
  INITCAP(REPLACE(marketplace, '_', ' ')) AS marketplace,
  last_order_at,
  last_finance_at,
  last_ads_at,
  last_stock_at,
  last_metric_at,
  orders_rows,
  finance_rows,
  ads_rows,
  stock_rows,
  metric_rows
FROM public.marketplace_data_freshness
WHERE marketplace = 'yandex_market';
