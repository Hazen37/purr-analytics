-- Metabase native question export
-- card_id: 109
-- card_name: Статистика по промокодам
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Ozon`

SELECT
  promo_code,
  MIN(order_date)::date AS first_order_date_with_promo,
  MAX(order_date)::date AS last_order_date_with_promo,
  COUNT(DISTINCT order_group_id) AS applied_count
FROM public.orders_clean
WHERE promo_code IS NOT NULL
  AND TRIM(promo_code) <> ''
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{flavor}}]]
  [[AND {{grams}}]]
GROUP BY promo_code
ORDER BY first_order_date_with_promo DESC;
