WITH sales AS (
  SELECT
    moi.sku,
    MAX(moi.product_name) AS product_name,
    SUM(moi.quantity) AS sold_units
  FROM public.marketplace_order_items moi
  WHERE moi.marketplace = 'wildberries'
    AND moi.order_date >= CURRENT_DATE - ({{days_back}}::int || ' days')::interval
  GROUP BY 1
),
stocks AS (
  SELECT
    sc.sku,
    sc.warehouse_name,
    SUM(sc.quantity_available) AS quantity_available,
    SUM(COALESCE(sc.quantity_reserved, 0)) AS quantity_reserved,
    SUM(COALESCE(sc.quantity_in_transit, 0)) AS quantity_in_transit
  FROM public.marketplace_stocks_current sc
  WHERE sc.marketplace = 'wildberries'
    [[AND {{warehouse_name}}]]
  GROUP BY 1, 2
)
SELECT
  stocks.sku AS sku,
  COALESCE(sales.product_name, '(без названия)') AS product_name,
  stocks.warehouse_name,
  COALESCE(stocks.quantity_available, 0) AS quantity_available,
  COALESCE(stocks.quantity_reserved, 0) AS quantity_reserved,
  COALESCE(stocks.quantity_in_transit, 0) AS quantity_in_transit,
  COALESCE(sales.sold_units, 0) AS sold_units_last_period
FROM stocks
LEFT JOIN sales ON sales.sku = stocks.sku
ORDER BY sold_units_last_period DESC, quantity_available DESC
LIMIT 200;
