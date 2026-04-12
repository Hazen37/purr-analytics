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
  CASE
  WHEN mfi.marketplace = 'ozon' THEN 'Ozon'
  WHEN mfi.marketplace = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN mfi.marketplace = 'wildberries' THEN 'Wildberries'
  ELSE mfi.marketplace
END AS marketplace,
  ABS(SUM(mfi.amount)) AS amount
FROM public.marketplace_finance_items mfi
WHERE 1=1
  AND mfi.happened_at IS NOT NULL
  [[AND {{happened_at}}]]
  [[AND {{finance_marketplace}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mfi.order_id
      AND moi.marketplace = mfi.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mfi.order_id
      AND moi.marketplace = mfi.marketplace
      AND {{grams}}
  )]]
GROUP BY 1, 2
ORDER BY 1, 2;
