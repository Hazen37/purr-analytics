-- Metabase native question export
-- card_id: 153
-- card_name: Прибыль
-- query_type: native
-- display: scalar
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Общее`

WITH revenue_agg AS (
  SELECT COALESCE(SUM(revenue), 0) AS revenue_total
  FROM public.marketplace_orders mo
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
finance_agg AS (
  SELECT COALESCE(SUM(mfi.amount), 0) AS commissions_total
  FROM public.marketplace_finance_items mfi
  JOIN public.marketplace_orders mo
    ON mo.marketplace = mfi.marketplace
   AND mo.order_id = mfi.order_id
  WHERE mo.status = 'delivered'
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
)
SELECT revenue_total - commissions_total AS profit_total
FROM revenue_agg
CROSS JOIN finance_agg;
