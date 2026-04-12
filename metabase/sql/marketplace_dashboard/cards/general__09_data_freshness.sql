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
ORDER BY marketplace;
