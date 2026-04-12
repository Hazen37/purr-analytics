-- Metabase native question export
-- card_id: 126
-- card_name: Дополнительные услуги Яндекс Маркет
-- query_type: native
-- display: area
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Яндекс Маркет`

SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day' THEN 'day'
      WHEN {{granularity}} = 'week' THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year' THEN 'year'
      ELSE 'week'
    END,
    mfi.happened_at
  )::date AS period,
  COALESCE(NULLIF(TRIM(mfi.fee_name), ''), 'Прочие комиссии') AS fee_name,
  ABS(SUM(mfi.amount)) AS amount
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = 'yandex_market'
  [[AND {{happened_at}}]]
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
