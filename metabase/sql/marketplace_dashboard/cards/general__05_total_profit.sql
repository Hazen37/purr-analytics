WITH base_orders AS (
  SELECT
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
    [[AND {{status}}]]
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
),
finance_agg AS (
  SELECT
    mfi.marketplace,
    mfi.order_id,
    COALESCE(SUM(mfi.amount), 0) AS commissions_total
  FROM public.marketplace_finance_items mfi
  GROUP BY 1, 2
)
SELECT COALESCE(
  SUM(
    CASE
      WHEN b.marketplace = 'ozon' THEN COALESCE(b.ozon_payout, b.revenue)
      ELSE b.revenue - COALESCE(f.commissions_total, 0)
    END
  ),
  0
) AS profit_total
FROM base_orders b
LEFT JOIN finance_agg f
  ON f.marketplace = b.marketplace
 AND f.order_id = b.order_id;
