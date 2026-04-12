-- Metabase native question export
-- card_id: 72
-- card_name: Marketplace / Яндекс Маркет / Расходы
-- query_type: native
-- display: bar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

WITH expense_rows AS (
  SELECT
    COALESCE(NULLIF(fee_group, ''), fee_name, 'other') AS expense_type,
    SUM(amount) AS amount
  FROM public.marketplace_finance_items
  WHERE marketplace = 'yandex_market'
  GROUP BY 1

  UNION ALL

  SELECT
    'ads' AS expense_type,
    SUM(spend) AS amount
  FROM public.marketplace_ads_daily
  WHERE marketplace = 'yandex_market'
  GROUP BY 1
)
SELECT
  expense_type,
  SUM(amount) AS amount
FROM expense_rows
GROUP BY 1
ORDER BY 2 DESC;
