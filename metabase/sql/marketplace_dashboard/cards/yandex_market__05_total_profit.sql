WITH revenue_agg AS (
  SELECT COALESCE(SUM(revenue), 0) AS revenue_total
  FROM public.marketplace_orders mo
  WHERE mo.marketplace = 'yandex_market'
    [[AND {{order_date}}]]
    AND mo.status = 'delivered'
),
finance_agg AS (
  SELECT COALESCE(SUM(amount), 0) AS commissions_total
  FROM public.marketplace_finance_items mfi
  WHERE mfi.marketplace = 'yandex_market'
    [[AND {{happened_at}}]]
),
ads_agg AS (
  SELECT COALESCE(SUM(spend), 0) AS ads_total
  FROM public.marketplace_ads_daily mad
  WHERE mad.marketplace = 'yandex_market'
    [[AND {{stat_date}}]]
)
SELECT
  revenue_total - commissions_total - ads_total AS profit_total
FROM revenue_agg
CROSS JOIN finance_agg
CROSS JOIN ads_agg;
