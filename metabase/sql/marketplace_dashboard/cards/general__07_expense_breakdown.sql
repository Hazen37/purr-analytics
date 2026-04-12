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
