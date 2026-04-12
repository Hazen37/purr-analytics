-- Metabase native question export
-- card_id: 46
-- card_name: Доли комиссий от выручки
-- query_type: native
-- display: area
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics OZON` (id=3), tab `Аналитика заказов`
--   - dashboard `PURR Analytics OZON` (id=3), tab `Затраты`

WITH agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day'   THEN 'day'
        WHEN {{granularity}} = 'week'  THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year'  THEN 'year'
        ELSE 'week'
      END,
      o.order_date
    )::date AS period,

    SUM(o.ozon_sale_commission) AS ozon_sale_commission,
    SUM(o.ozon_discount)        AS ozon_discount,
    SUM(o.ozon_delivery_fee)    AS ozon_delivery_fee,
    SUM(o.ozon_acquiring_fee)   AS ozon_acquiring_fee,
    SUM(o.ozon_ads_fee)         AS ozon_ads_fee,
    SUM(o.ozon_other_fee_real)  AS ozon_other_fee_real,

    SUM(o.profit)               AS profit
  FROM public.orders_clean o
  WHERE 1=1
    [[AND {{order_date}}]]
    AND o.status = 'delivered'
    AND EXISTS (
      SELECT 1
      FROM public.order_items oi
      JOIN public.products p ON p.sku = oi.sku
      WHERE oi.order_id = o.order_id
        [[AND {{flavor}}]]
        [[AND {{grams}}]]
    )
  GROUP BY 1
)

SELECT period, 'Комиссия площадки'    AS metric, ABS(ozon_sale_commission) AS amount FROM agg
UNION ALL
SELECT period, 'Скидки'               AS metric, ABS(ozon_discount)        AS amount FROM agg
UNION ALL
SELECT period, 'Доставка'             AS metric, ABS(ozon_delivery_fee)    AS amount FROM agg
UNION ALL
SELECT period, 'Эквайринг'            AS metric, ABS(ozon_acquiring_fee)   AS amount FROM agg
UNION ALL
SELECT period, 'Реклама (по заказам)' AS metric, ABS(ozon_ads_fee)         AS amount FROM agg
UNION ALL
SELECT period, 'Прочие комиссии'      AS metric, ABS(ozon_other_fee_real)  AS amount FROM agg
UNION ALL
SELECT period, 'Прибыль'              AS metric, profit                    AS amount FROM agg
ORDER BY period, metric;
