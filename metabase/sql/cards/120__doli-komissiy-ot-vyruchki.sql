-- Metabase native question export
-- card_id: 120
-- card_name: Доли комиссий от выручки
-- query_type: native
-- display: area
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - dashboard `PURR Analytics Marketplace` (id=4), tab `Яндекс Маркет`

WITH revenue_agg AS (
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
    SUM(mo.revenue) AS revenue
  FROM public.marketplace_orders mo
  WHERE mo.marketplace = 'yandex_market'
    [[AND {{order_date}}]]
    AND mo.status = 'delivered'
  GROUP BY 1
),
finance_agg AS (
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
    SUM(mfi.amount) AS finance_total
  FROM public.marketplace_finance_items mfi
  WHERE mfi.marketplace = 'yandex_market'
    [[AND {{happened_at}}]]
  GROUP BY 1
),
ads_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day' THEN 'day'
        WHEN {{granularity}} = 'week' THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mad.stat_date
    )::date AS period,
    SUM(mad.spend) AS ads_total
  FROM public.marketplace_ads_daily mad
  WHERE mad.marketplace = 'yandex_market'
    [[AND {{stat_date}}]]
  GROUP BY 1
),
periods AS (
  SELECT period FROM revenue_agg
  UNION
  SELECT period FROM finance_agg
  UNION
  SELECT period FROM ads_agg
),
base AS (
  SELECT
    p.period,
    COALESCE(r.revenue, 0) AS revenue,
    COALESCE(f.finance_total, 0) AS finance_total,
    COALESCE(a.ads_total, 0) AS ads_total
  FROM periods p
  LEFT JOIN revenue_agg r ON r.period = p.period
  LEFT JOIN finance_agg f ON f.period = p.period
  LEFT JOIN ads_agg a ON a.period = p.period
)
SELECT period, 'Комиссия площадки' AS metric, finance_total AS amount FROM base
UNION ALL
SELECT period, 'Реклама (по заказам)' AS metric, ads_total AS amount FROM base
UNION ALL
SELECT period, 'Прибыль' AS metric, revenue - finance_total - ads_total AS amount FROM base
ORDER BY period, metric;
