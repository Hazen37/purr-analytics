-- Metabase native question export
-- card_id: 142
-- card_name: Статистика по комиссиям
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Wildberries`

SELECT
  COALESCE(NULLIF(TRIM(fee_name), ''), '(не указан)') AS promo_code,
  MIN(happened_at)::date AS first_order_date_with_promo,
  MAX(happened_at)::date AS last_order_date_with_promo,
  COUNT(*) AS applied_count
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = 'wildberries'
  [[AND {{happened_at}}]]
GROUP BY 1
ORDER BY first_order_date_with_promo DESC NULLS LAST;
