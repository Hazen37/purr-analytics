-- Metabase native question export
-- card_id: 117
-- card_name: Средний чек
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Яндекс Маркет`

SELECT
  ROUND(COALESCE(AVG(revenue), 0), 2) AS average_order_value
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  AND marketplace = 'yandex_market'
;
