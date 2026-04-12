-- Metabase native question export
-- card_id: 159
-- card_name: Прибыль по площадкам по дням
-- query_type: native
-- display: line
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Общее`

WITH revenue_agg AS (
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
    SUM(mo.revenue) AS revenue_total
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
  GROUP BY 1, 2
),
costs_agg AS (
  SELECT
    period,
    marketplace,
    SUM(amount) AS amount_total
  FROM (
    SELECT
      date_trunc(
        CASE
          WHEN {{granularity}} = 'day' THEN 'day'
          WHEN {{granularity}} = 'week' THEN 'week'
          WHEN {{granularity}} = 'month' THEN 'month'
          WHEN {{granularity}} = 'year' THEN 'year'
          ELSE 'day'
        END,
        mfi.happened_at
      )::date AS period,
      mfi.marketplace,
      mfi.amount
    FROM public.marketplace_finance_items mfi
    WHERE 1=1
      [[AND {{happened_at}}]]
      [[AND {{finance_marketplace}}]]

    UNION ALL

    SELECT
      date_trunc(
        CASE
          WHEN {{granularity}} = 'day' THEN 'day'
          WHEN {{granularity}} = 'week' THEN 'week'
          WHEN {{granularity}} = 'month' THEN 'month'
          WHEN {{granularity}} = 'year' THEN 'year'
          ELSE 'day'
        END,
        mad.stat_date
      )::date AS period,
      mad.marketplace,
      mad.spend AS amount
    FROM public.marketplace_ads_daily mad
    WHERE 1=1
      [[AND {{stat_date}}]]
      [[AND {{ads_marketplace}}]]
  ) src
  GROUP BY 1, 2
)
SELECT
  COALESCE(r.period, c.period) AS period,
  CASE
  WHEN COALESCE(r.marketplace, c.marketplace) = 'ozon' THEN 'Ozon'
  WHEN COALESCE(r.marketplace, c.marketplace) = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN COALESCE(r.marketplace, c.marketplace) = 'wildberries' THEN 'Wildberries'
  ELSE COALESCE(r.marketplace, c.marketplace)
END AS marketplace,
  COALESCE(r.revenue_total, 0) - COALESCE(c.amount_total, 0) AS profit_total
FROM revenue_agg r
FULL OUTER JOIN costs_agg c
  ON c.period = r.period AND c.marketplace = r.marketplace
ORDER BY 1, 2;
