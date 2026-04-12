WITH base_orders AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day' THEN 'day'
        WHEN {{granularity}} = 'week' THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year' THEN 'year'
        ELSE 'day'
      END,
      mo.order_date
    )::date AS period,
    mo.marketplace,
    mo.order_id,
    mo.revenue,
    o.ozon_payout,
    o.ozon_fees_total
  FROM public.marketplace_orders mo
  LEFT JOIN public.orders o
    ON mo.marketplace = 'ozon'
   AND o.order_id = mo.order_id
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
    AND mo.status = 'delivered'
),
order_finance AS (
  SELECT
    b.marketplace,
    b.order_id,
    COALESCE(SUM(mfi.amount), 0) AS order_commissions
  FROM base_orders b
  LEFT JOIN public.marketplace_finance_items mfi
    ON mfi.marketplace = b.marketplace
   AND mfi.order_id = b.order_id
  GROUP BY 1, 2
),
period_marketplace_agg AS (
  SELECT
    b.period,
    b.marketplace,
    SUM(
      CASE
        WHEN b.marketplace = 'ozon' THEN b.revenue - ABS(COALESCE(b.ozon_fees_total, 0))
        ELSE b.revenue - COALESCE(f.order_commissions, 0)
      END
    ) AS profit_total
  FROM base_orders b
  LEFT JOIN order_finance f
    ON f.marketplace = b.marketplace
   AND f.order_id = b.order_id
  GROUP BY 1, 2
)
SELECT
  period,
  CASE
  WHEN marketplace = 'ozon' THEN 'Ozon'
  WHEN marketplace = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN marketplace = 'wildberries' THEN 'Wildberries'
  ELSE marketplace
END AS marketplace,
  profit_total
FROM period_marketplace_agg
ORDER BY 1, 2;
