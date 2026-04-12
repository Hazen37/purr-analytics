-- Metabase native question export
-- card_id: 70
-- card_name: Marketplace / Яндекс Маркет / Средний чек
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  ROUND(COALESCE(AVG(revenue), 0), 2) AS average_order_value
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'yandex_market'
;
