-- Metabase native question export
-- card_id: 64
-- card_name: Marketplace / Общее / Расходы по площадкам
-- query_type: native
-- display: bar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

WITH expense_rows AS (
  SELECT
    marketplace,
    COALESCE(NULLIF(fee_group, ''), fee_name, 'other') AS expense_type,
    SUM(amount) AS amount
  FROM public.marketplace_finance_items
  GROUP BY 1, 2

  UNION ALL

  SELECT
    marketplace,
    'ads' AS expense_type,
    SUM(spend) AS amount
  FROM public.marketplace_ads_daily
  GROUP BY 1
)
SELECT
  marketplace,
  expense_type,
  SUM(amount) AS amount
FROM expense_rows
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
