-- Metabase native question export
-- card_id: 127
-- card_name: Периодические комиссии
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Яндекс Маркет`

SELECT
  happened_at::date AS cost_date,
  COALESCE(NULLIF(TRIM(fee_group), ''), 'marketplace') AS fee_group,
  COALESCE(NULLIF(TRIM(fee_name), ''), '(не указан)') AS fee_name,
  -ABS(amount) AS amount
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = 'yandex_market'
  [[AND {{happened_at}}]]
  AND amount <> 0
ORDER BY cost_date DESC, fee_group, fee_name;
