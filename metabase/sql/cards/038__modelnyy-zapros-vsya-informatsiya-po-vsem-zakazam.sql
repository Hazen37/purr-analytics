-- Metabase native question export
-- card_id: 38
-- card_name: Модельный запрос - Вся информация по всем заказам
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T12:05:12Z
-- dashboard_usage:
--   - not used on dashboards

-- Metabase native question export
-- card_id: 38
-- card_name: Модельный запрос - Вся информация по всем заказам
-- query_type: native
-- display: table
-- collection: Unknown
-- extracted_at_utc: 2026-04-12T08:34:21Z
-- dashboard_usage:
--   - not used on dashboards

SELECT
  "source"."order_id" AS "order_id",
  "source"."customer_id" AS "customer_id",
  "source"."order_group_id" AS "order_group_id",
  "source"."order_date" AS "order_date",
  "source"."status" AS "status",
  "source"."revenue" AS "revenue",
  "source"."ozon_fees_total" AS "ozon_fees_total",
  "source"."ozon_payout" AS "ozon_payout",
  "source"."campaign" AS "campaign",
  "source"."fulfillment_type" AS "fulfillment_type",
  -- "source"."is_first_order" AS "is_first_order",
  "source"."order_num_delivered" AS "order_num_delivered",
  "source"."order_group_num_delivered" AS "order_group_num_delivered",

  -- NEW:
  "source"."delivery_city" AS "delivery_city",
  "source"."warehouse_name" AS "warehouse_name",
  "source"."cluster_to" AS "cluster_to",
  "source"."promo_code" AS "promo_code",
  "source"."ozon_actions" AS "ozon_actions",

  "source"."sku" AS "sku",
  "source"."quantity" AS "quantity",
  "source"."price" AS "price",
  "source"."item_revenue" AS "item_revenue",
  "source"."flavor" AS "flavor",
  "source"."grams" AS "grams"
FROM
  (
    SELECT
      "source"."order_id" AS "order_id",
      "source"."customer_id" AS "customer_id",
      "source"."order_group_id" AS "order_group_id",
      "source"."order_date" AS "order_date",
      "source"."status" AS "status",
      "source"."revenue" AS "revenue",
      "source"."ozon_fees_total" AS "ozon_fees_total",
      "source"."ozon_payout" AS "ozon_payout",
      "source"."campaign" AS "campaign",
      "source"."fulfillment_type" AS "fulfillment_type",
      -- "source"."is_first_order" AS "is_first_order",
      "source"."order_num_delivered" AS "order_num_delivered",
      "source"."order_group_num_delivered" AS "order_group_num_delivered",

      -- NEW:
      "source"."delivery_city" AS "delivery_city",
      "source"."warehouse_name" AS "warehouse_name",
      "source"."cluster_to" AS "cluster_to",
      "source"."promo_code" AS "promo_code",
      "source"."ozon_actions" AS "ozon_actions",

      "source"."sku" AS "sku",
      "source"."quantity" AS "quantity",
      "source"."price" AS "price",
      "source"."item_revenue" AS "item_revenue",
      "source"."flavor" AS "flavor",
      "source"."grams" AS "grams"
    FROM
      (
        SELECT
          o.order_id,
          o.customer_id,
          o.order_group_id,
          o.order_date,
          o.status,
          o.revenue,
          o.ozon_fees_total,
          o.ozon_payout,
          o.campaign,
          o.fulfillment_type,
          -- o.is_first_order,
          o.order_num_delivered,
          o.order_group_num_delivered,

          -- NEW (берём прямо из orders_clean):
          o.delivery_city,
          o.warehouse_name,
          o.cluster_to,
          o.promo_code,
          o.ozon_actions,

          oi.sku,
          oi.quantity,
          oi.price,
          oi.revenue AS item_revenue,
          p.flavor,
          p.grams
        FROM
          orders_clean o
          LEFT JOIN order_items oi ON oi.order_id = o.order_id
          LEFT JOIN products p ON p.sku = oi.sku
      ) AS "source"
    ORDER BY
      "source"."status" ASC
  ) AS "source"
LIMIT
  1048575;
