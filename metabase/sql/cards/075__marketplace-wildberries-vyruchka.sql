-- Metabase native question export
-- card_id: 75
-- card_name: Marketplace / Wildberries / Выручка
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  COALESCE(SUM(revenue), 0) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'wildberries'
;
