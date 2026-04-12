WITH params AS (
  SELECT
    CURRENT_DATE::date AS date_to,
    (CURRENT_DATE - ({{days_back}} || ' days')::interval)::date AS date_from
),
sales AS (
  SELECT
    oi.sku,
    SUM(oi.quantity) AS units_sold,
    COUNT(DISTINCT o.order_date::date) AS active_days
  FROM public.order_items oi
  JOIN public.orders o ON o.order_id = oi.order_id
  JOIN params p ON TRUE
  WHERE o.status <> 'cancelled'
    AND o.order_date::date BETWEEN p.date_from AND p.date_to
  GROUP BY 1
),
stock AS (
  SELECT
    sku,
    SUM(free_to_sell) AS stock_free
  FROM public.stocks_current
  GROUP BY 1
),
days AS (
  SELECT
    (p.date_to - p.date_from + 1) AS calendar_days
  FROM params p
)
SELECT
  pr.sku,
  pr.name,
  pr.flavor,
  pr.grams,
  COALESCE(st.stock_free, 0) AS stock_free,
  COALESCE(s.units_sold, 0)  AS units_sold,
  COALESCE(s.active_days, 0) AS active_days,
  d.calendar_days            AS calendar_days,
  CASE
    WHEN COALESCE(s.active_days, 0) = 0 THEN NULL
    ELSE COALESCE(s.units_sold, 0)::numeric / s.active_days
  END AS avg_daily_units_active,
  (COALESCE(s.units_sold, 0)::numeric / NULLIF(d.calendar_days, 0)) AS avg_daily_units_calendar,
  CASE
    WHEN COALESCE(s.active_days, 0) = 0 THEN NULL
    ELSE COALESCE(st.stock_free, 0)::numeric
         / NULLIF(COALESCE(s.units_sold, 0)::numeric / s.active_days, 0)
  END AS days_cover_active,
  CASE
    WHEN d.calendar_days = 0 THEN NULL
    ELSE COALESCE(st.stock_free, 0)::numeric
         / NULLIF(COALESCE(s.units_sold, 0)::numeric / d.calendar_days, 0)
  END AS days_cover_calendar
FROM public.products pr
CROSS JOIN days d
LEFT JOIN sales s ON s.sku = pr.sku
LEFT JOIN stock st ON st.sku = pr.sku
ORDER BY days_cover_calendar NULLS LAST, stock_free DESC;
