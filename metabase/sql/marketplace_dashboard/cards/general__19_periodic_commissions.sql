SELECT
  CASE
  WHEN mfi.marketplace = 'ozon' THEN 'Ozon'
  WHEN mfi.marketplace = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN mfi.marketplace = 'wildberries' THEN 'Wildberries'
  ELSE mfi.marketplace
END AS marketplace,
  mfi.happened_at::date AS cost_date,
  COALESCE(NULLIF(TRIM(mfi.fee_group), ''), 'marketplace') AS fee_group,
  COALESCE(NULLIF(TRIM(mfi.fee_name), ''), '(не указан)') AS fee_name,
  -ABS(mfi.amount) AS amount
FROM public.marketplace_finance_items mfi
WHERE 1=1
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
  AND mfi.amount <> 0
ORDER BY cost_date DESC, marketplace, fee_group, fee_name;
