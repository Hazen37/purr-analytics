SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day' THEN 'day'
      WHEN {{granularity}} = 'week' THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year' THEN 'year'
      ELSE 'week'
    END,
    mo.order_date
  )::date AS period,
  marketplace,
  COUNT(DISTINCT order_id) AS orders_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
GROUP BY 1, 2
ORDER BY 1, 2;
