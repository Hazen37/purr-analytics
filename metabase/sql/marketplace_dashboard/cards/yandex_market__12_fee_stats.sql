SELECT
  COALESCE(NULLIF(TRIM(fee_name), ''), '(не указан)') AS promo_code,
  MIN(happened_at)::date AS first_order_date_with_promo,
  MAX(happened_at)::date AS last_order_date_with_promo,
  COUNT(*) AS applied_count
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = 'yandex_market'
  [[AND {{happened_at}}]]
GROUP BY 1
ORDER BY first_order_date_with_promo DESC NULLS LAST;
