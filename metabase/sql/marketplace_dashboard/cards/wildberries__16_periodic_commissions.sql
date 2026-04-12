SELECT
  happened_at::date AS cost_date,
  COALESCE(NULLIF(TRIM(fee_group), ''), 'marketplace') AS fee_group,
  COALESCE(NULLIF(TRIM(fee_name), ''), '(не указан)') AS fee_name,
  -ABS(amount) AS amount
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = 'wildberries'
  [[AND {{happened_at}}]]
  AND amount <> 0
ORDER BY cost_date DESC, fee_group, fee_name;
