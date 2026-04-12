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
