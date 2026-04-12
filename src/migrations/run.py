# src/migrations/run.py
"""
Упрощённые миграции: создаём таблицы и "догоняем" схему через
ALTER TABLE ... ADD COLUMN IF NOT EXISTS.

Запуск:
  python -m src.migrations.run
"""

from __future__ import annotations

from src.core.db import execute_query


# -----------------------------
# Core tables
# -----------------------------

def create_reviews_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ozon_reviews (
            review_id BIGINT PRIMARY KEY,

            -- связь с товаром
            sku BIGINT,
            product_id BIGINT,
            offer_id TEXT,
            product_name TEXT,

            -- сам отзыв
            rating INT,
            review_text TEXT,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            status TEXT,

            -- реакции (если есть)
            likes_count INT,
            dislikes_count INT,

            -- техническое
            loaded_at TIMESTAMP DEFAULT now(),
            raw JSONB
        );
        """
    )

    execute_query("CREATE INDEX IF NOT EXISTS idx_ozon_reviews_sku ON ozon_reviews(sku);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_ozon_reviews_product_id ON ozon_reviews(product_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_ozon_reviews_published_at ON ozon_reviews(published_at);")

def create_customers_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            first_order_date DATE,
            last_order_date DATE,
            orders_count INT,
            total_revenue NUMERIC
        );
        """
    )


def create_orders_table() -> None:
    """
    Базовая таблица заказов + догоняем колонки, которые добавлялись позже.
    """
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_date TIMESTAMP,
            revenue NUMERIC,
            campaign TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """
    )

    # Базовые индексы для фильтраций/джойнов
    execute_query("CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);")

    # "догоняем" обязательные колонки, которые появились позже в ETL/дашбордах
    execute_query(
        """
        ALTER TABLE orders
          ADD COLUMN IF NOT EXISTS status TEXT,
          ADD COLUMN IF NOT EXISTS ozon_fees_total NUMERIC,
          ADD COLUMN IF NOT EXISTS ozon_payout NUMERIC,
          ADD COLUMN IF NOT EXISTS sales_report NUMERIC,
          ADD COLUMN IF NOT EXISTS fulfillment_type TEXT,

          ADD COLUMN IF NOT EXISTS order_num_delivered INT,
          ADD COLUMN IF NOT EXISTS order_group_id TEXT,
          ADD COLUMN IF NOT EXISTS order_group_num_delivered INT,

          ADD COLUMN IF NOT EXISTS ozon_delivery_fee NUMERIC,
          ADD COLUMN IF NOT EXISTS ozon_acquiring_fee NUMERIC,
          ADD COLUMN IF NOT EXISTS ozon_ads_fee NUMERIC,

          ADD COLUMN IF NOT EXISTS campaign_id TEXT,
          ADD COLUMN IF NOT EXISTS campaign_title TEXT,
          ADD COLUMN IF NOT EXISTS ozon_ads_attributed NUMERIC,

          ADD COLUMN IF NOT EXISTS ozon_sale_commission NUMERIC,
          ADD COLUMN IF NOT EXISTS ozon_discount NUMERIC,
          ADD COLUMN IF NOT EXISTS ozon_other_fee_real NUMERIC,
          ADD COLUMN IF NOT EXISTS profit NUMERIC,

          ADD COLUMN IF NOT EXISTS delivery_city TEXT,
          ADD COLUMN IF NOT EXISTS warehouse_name TEXT,
          ADD COLUMN IF NOT EXISTS buyer_name TEXT,
          ADD COLUMN IF NOT EXISTS promo_code TEXT,
          ADD COLUMN IF NOT EXISTS ozon_actions TEXT,
          ADD COLUMN IF NOT EXISTS cluster_from TEXT,
          ADD COLUMN IF NOT EXISTS cluster_to TEXT,

          ADD COLUMN IF NOT EXISTS ozon_missing BOOLEAN DEFAULT false,
          ADD COLUMN IF NOT EXISTS ozon_missing_at TIMESTAMP;
        """
    )

    # используем orders_clean как VIEW (пересоздаём, потому что OR REPLACE не умеет "убирать" колонки)
    execute_query("DROP VIEW IF EXISTS public.orders_clean;")

    execute_query("""
        CREATE VIEW public.orders_clean AS
        SELECT
        order_id,
        customer_id,
        order_date,
        revenue,
        campaign,
        status,
        fulfillment_type,
        ozon_fees_total,
        ozon_payout,
        sales_report,
        ozon_delivery_fee,
        ozon_acquiring_fee,
        ozon_ads_fee,
        campaign_id,
        campaign_title,
        ozon_ads_attributed,
        ozon_sale_commission,
        ozon_discount,
        ozon_other_fee_real,
        profit,
        delivery_city,
        warehouse_name,
        buyer_name,
        promo_code,
        ozon_actions,
        cluster_from,
        cluster_to,
        ozon_missing,
        ozon_missing_at,
        order_num_delivered,
        order_group_id,
        order_group_num_delivered
        FROM public.orders
        WHERE customer_id <> '47533921';
    """)

    # функция пересчёта нумерации заказов (кроме cancelled)
    execute_query("""
        CREATE OR REPLACE FUNCTION public.recalc_order_numbers()
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
        WITH base AS (
            SELECT
            order_id,
            customer_id,
            order_date,
            COALESCE(order_group_id, order_id) AS group_key
            FROM public.orders
            WHERE status <> 'cancelled'
        ),
        grouped AS (
            SELECT
            order_id,
            customer_id,
            order_date,
            group_key,
            MIN(order_date) OVER (PARTITION BY customer_id, group_key) AS group_first_date
            FROM base
        ),
        calc AS (
            SELECT
            order_id,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY order_date, order_id
            ) AS order_num_delivered,
            DENSE_RANK() OVER (
                PARTITION BY customer_id
                ORDER BY group_first_date, group_key
            ) AS order_group_num_delivered
            FROM grouped
        )
        UPDATE public.orders o
        SET
            order_num_delivered = c.order_num_delivered,
            order_group_num_delivered = c.order_group_num_delivered
        FROM calc c
        WHERE o.order_id = c.order_id;

        UPDATE public.orders
        SET
            order_num_delivered = NULL,
            order_group_num_delivered = NULL
        WHERE status = 'cancelled';
        END;
        $$;
    """)



def create_products_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS products (
            sku   BIGINT PRIMARY KEY,
            name  TEXT,
            flavor TEXT,
            grams  INT
        );
        """
    )


def create_canonical_products_tables() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS canonical_products (
            canonical_product_id BIGSERIAL PRIMARY KEY,
            canonical_key TEXT UNIQUE,
            canonical_name TEXT NOT NULL,
            flavor TEXT,
            grams INT,
            seed_marketplace TEXT,
            seed_external_key TEXT,
            source TEXT NOT NULL DEFAULT 'manual',
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            updated_at TIMESTAMP NOT NULL DEFAULT now(),
            UNIQUE (seed_marketplace, seed_external_key)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_canonical_products_flavor ON canonical_products(flavor);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_canonical_products_grams ON canonical_products(grams);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS marketplace_product_mapping (
            mapping_id BIGSERIAL PRIMARY KEY,
            marketplace TEXT NOT NULL,
            external_key_type TEXT NOT NULL,
            external_key TEXT NOT NULL,
            canonical_product_id BIGINT NOT NULL REFERENCES canonical_products(canonical_product_id) ON DELETE CASCADE,
            source TEXT NOT NULL DEFAULT 'manual',
            confidence NUMERIC(5, 4) NOT NULL DEFAULT 1.0,
            notes TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            updated_at TIMESTAMP NOT NULL DEFAULT now(),
            UNIQUE (marketplace, external_key_type, external_key)
        );
        """
    )
    execute_query(
        "CREATE INDEX IF NOT EXISTS idx_marketplace_product_mapping_canonical "
        "ON marketplace_product_mapping(canonical_product_id);"
    )


def seed_ozon_canonical_products() -> None:
    execute_query(
        """
        INSERT INTO canonical_products (
            canonical_key,
            canonical_name,
            flavor,
            grams,
            seed_marketplace,
            seed_external_key,
            source,
            updated_at
        )
        SELECT
            CONCAT('ozon-sku-', p.sku::text) AS canonical_key,
            COALESCE(NULLIF(TRIM(p.name), ''), p.sku::text) AS canonical_name,
            NULLIF(TRIM(p.flavor), '') AS flavor,
            p.grams,
            'ozon' AS seed_marketplace,
            p.sku::text AS seed_external_key,
            'ozon_seed' AS source,
            now() AS updated_at
        FROM public.products p
        ON CONFLICT (seed_marketplace, seed_external_key)
        DO UPDATE SET
            canonical_key = EXCLUDED.canonical_key,
            canonical_name = EXCLUDED.canonical_name,
            flavor = EXCLUDED.flavor,
            grams = EXCLUDED.grams,
            source = EXCLUDED.source,
            updated_at = now();
        """
    )

    execute_query(
        """
        INSERT INTO marketplace_product_mapping (
            marketplace,
            external_key_type,
            external_key,
            canonical_product_id,
            source,
            confidence,
            updated_at
        )
        SELECT
            'ozon' AS marketplace,
            'sku' AS external_key_type,
            p.sku::text AS external_key,
            cp.canonical_product_id,
            'ozon_seed' AS source,
            1.0 AS confidence,
            now() AS updated_at
        FROM public.products p
        JOIN canonical_products cp
          ON cp.seed_marketplace = 'ozon'
         AND cp.seed_external_key = p.sku::text
        ON CONFLICT (marketplace, external_key_type, external_key)
        DO UPDATE SET
            canonical_product_id = EXCLUDED.canonical_product_id,
            source = EXCLUDED.source,
            confidence = EXCLUDED.confidence,
            updated_at = now();
        """
    )


def create_order_items_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS order_items (
            id SERIAL PRIMARY KEY,
            order_id TEXT REFERENCES orders(order_id) ON DELETE CASCADE,
            sku BIGINT REFERENCES products(sku),
            quantity INT,
            price NUMERIC,
            revenue NUMERIC
        );
        """
    )

    execute_query("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);")


def create_order_fee_items_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS order_fee_items (
          id BIGSERIAL PRIMARY KEY,
          order_id TEXT REFERENCES orders(order_id) ON DELETE CASCADE,
          fee_group TEXT,
          fee_name TEXT,
          amount NUMERIC,
          percent NUMERIC,
          product_id BIGINT,
          source TEXT DEFAULT 'posting_financial'
        );
        """
    )

    execute_query("CREATE INDEX IF NOT EXISTS idx_order_fee_items_order_id ON order_fee_items(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_order_fee_items_group_name ON order_fee_items(fee_group, fee_name);")

    # uid для идемпотентного UPSERT (finance_api)
    execute_query(
        """
        ALTER TABLE order_fee_items
          ADD COLUMN IF NOT EXISTS uid TEXT;
        """
    )

    # обычный UNIQUE (без WHERE), чтобы ON CONFLICT(uid) работал
    # execute_query("DROP INDEX IF EXISTS ux_order_fee_items_uid;")
    execute_query(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_order_fee_items_uid
        ON order_fee_items(uid);
        """
    )


def create_ads_campaigns_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ads_campaigns (
            campaign_id TEXT,
            name TEXT,
            date DATE,
            clicks INT,
            impressions INT,
            spend NUMERIC,
            ozon_orders INT,
            ozon_revenue NUMERIC,
            PRIMARY KEY (campaign_id, date)
        );
        """
    )

    execute_query("CREATE INDEX IF NOT EXISTS idx_ads_campaigns_date ON ads_campaigns(date);")


# -----------------------------
# Performance / reporting tables
# -----------------------------

def create_perf_campaigns_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS perf_campaigns (
            campaign_id BIGINT PRIMARY KEY,
            title TEXT,
            state TEXT,
            adv_object_type TEXT,
            payment_type TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            raw JSONB
        );
        """
    )


def create_performance_campaign_daily_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS performance_campaign_daily (
          campaign_id    TEXT NOT NULL,
          campaign_title TEXT,
          stat_date      DATE NOT NULL,

          impressions    BIGINT,
          clicks         BIGINT,
          spend          NUMERIC,
          avg_bid        NUMERIC,
          orders_cnt     BIGINT,
          orders_amount  NUMERIC,

          PRIMARY KEY (campaign_id, stat_date)
        );
        """
    )

    execute_query("CREATE INDEX IF NOT EXISTS idx_pcd_stat_date ON performance_campaign_daily(stat_date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_pcd_campaign_id ON performance_campaign_daily(campaign_id);")


def create_performance_order_attribution_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS performance_order_attribution (
          id BIGSERIAL PRIMARY KEY,
          campaign_id TEXT,
          campaign_title TEXT,
          order_id TEXT,
          ext_order_id TEXT,
          sku BIGINT,
          offer_id TEXT,
          product_name TEXT,
          stat_date DATE NOT NULL,
          price NUMERIC,
          amount NUMERIC,
          spent NUMERIC,
          bid NUMERIC,
          bid_percent NUMERIC,
          qty INT
        );
        """
    )

    execute_query("CREATE INDEX IF NOT EXISTS idx_poa_order_id ON performance_order_attribution(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_poa_stat_date ON performance_order_attribution(stat_date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_poa_campaign_date ON performance_order_attribution(campaign_id, stat_date);")


def create_finance_period_costs_table() -> None:
    """
    Для отчётов/дашбордов: периодные расходы по дням/группам/статьям.
    """
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS finance_period_costs (
          cost_date DATE NOT NULL,
          fee_group TEXT NOT NULL,
          fee_name  TEXT NOT NULL,
          amount    NUMERIC NOT NULL DEFAULT 0,
          PRIMARY KEY (cost_date, fee_group, fee_name)
        );
        """
    )

    execute_query("CREATE INDEX IF NOT EXISTS idx_finance_period_costs_date ON finance_period_costs(cost_date);")

def create_ozon_sku_day_metrics_table() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS public.ozon_sku_day_metrics (
          date date NOT NULL,
          sku  bigint NOT NULL,

          impressions integer NOT NULL DEFAULT 0,
          views       integer NOT NULL DEFAULT 0,
          cart_adds   integer NOT NULL DEFAULT 0,
          ordered_units integer NOT NULL DEFAULT 0,
          revenue     numeric(14,2) NOT NULL DEFAULT 0,

          loaded_at   timestamptz NOT NULL DEFAULT now(),

          PRIMARY KEY (date, sku)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ozon_sku_day_metrics_date ON public.ozon_sku_day_metrics(date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_ozon_sku_day_metrics_sku  ON public.ozon_sku_day_metrics(sku);")

def create_vw_ozon_sku_day_funnel() -> None:
    execute_query("DROP VIEW IF EXISTS public.vw_ozon_sku_day_funnel;")
    execute_query(
        """
        CREATE OR REPLACE VIEW public.vw_sku_day_business AS
        WITH orders_by_sku_day AS (
        SELECT
            DATE(order_date) AS date,
            oi.sku,
            SUM(oi.quantity) AS ordered_units,
            SUM(oi.revenue)  AS revenue
        FROM public.order_items oi
        JOIN public.orders o ON o.order_id = oi.order_id
        WHERE o.status = 'delivered'
        GROUP BY 1, 2
        )

        SELECT
        m.date,
        m.sku,
        p.name,
        p.flavor,
        p.grams,

        m.impressions,
        m.views,
        CASE WHEN m.impressions = 0 THEN NULL ELSE m.views::numeric / m.impressions END AS ctr,

        COALESCE(o.ordered_units, 0) AS ordered_units,
        COALESCE(o.revenue, 0)       AS revenue,

        CASE WHEN m.views = 0 THEN NULL ELSE o.ordered_units::numeric / m.views END AS cr,
        CASE WHEN m.views = 0 THEN NULL ELSE o.revenue / m.views END AS revenue_per_view

        FROM public.ozon_sku_day_metrics m
        LEFT JOIN orders_by_sku_day o
        ON o.date = m.date AND o.sku = m.sku
        LEFT JOIN public.products p
        ON p.sku = m.sku;
        """
    )

def create_stocks_current_table() -> None:
    execute_query("""
    CREATE TABLE IF NOT EXISTS public.stocks_current (
      sku BIGINT NOT NULL,
      warehouse_id BIGINT NOT NULL DEFAULT 0,
      warehouse_name TEXT,

      free_to_sell BIGINT NOT NULL DEFAULT 0,
      reserved     BIGINT NOT NULL DEFAULT 0,
      total        BIGINT NOT NULL DEFAULT 0,

      updated_at timestamptz NOT NULL DEFAULT now(),
      raw JSONB,

      PRIMARY KEY (sku, warehouse_id)
    );
    """)
    execute_query("CREATE INDEX IF NOT EXISTS idx_stocks_current_updated_at ON public.stocks_current(updated_at);")


def create_yandex_market_tables() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ym_orders (
            order_id TEXT PRIMARY KEY,
            campaign_id BIGINT,
            business_id BIGINT,
            order_date TIMESTAMP,
            status TEXT,
            currency TEXT,
            total_amount NUMERIC,
            warehouse_id TEXT,
            updated_at TIMESTAMP DEFAULT now(),
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_orders_order_date ON ym_orders(order_date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_orders_status ON ym_orders(status);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ym_order_items (
            id BIGSERIAL PRIMARY KEY,
            order_id TEXT REFERENCES ym_orders(order_id) ON DELETE CASCADE,
            offer_id TEXT,
            sku TEXT,
            name TEXT,
            quantity NUMERIC,
            price NUMERIC,
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_order_items_order_id ON ym_order_items(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_order_items_offer_id ON ym_order_items(offer_id);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ym_finance_items (
            id BIGSERIAL PRIMARY KEY,
            order_id TEXT REFERENCES ym_orders(order_id) ON DELETE CASCADE,
            fee_type TEXT,
            amount NUMERIC,
            currency TEXT,
            happened_at TIMESTAMP,
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_finance_items_order_id ON ym_finance_items(order_id);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ym_stocks_current (
            offer_id TEXT,
            sku TEXT,
            warehouse_id TEXT,
            warehouse_name TEXT,
            fit NUMERIC DEFAULT 0,
            freeze_qty NUMERIC DEFAULT 0,
            updated_at TIMESTAMP DEFAULT now(),
            raw JSONB,
            PRIMARY KEY (offer_id, warehouse_id)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_stocks_current_updated_at ON ym_stocks_current(updated_at);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ym_reviews (
            review_id TEXT PRIMARY KEY,
            order_id TEXT,
            rating NUMERIC,
            review_text TEXT,
            published_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now(),
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_reviews_published_at ON ym_reviews(published_at);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ym_sku_day_metrics (
            metric_date DATE NOT NULL,
            sku TEXT NOT NULL,
            impressions BIGINT DEFAULT 0,
            views BIGINT DEFAULT 0,
            ordered_units BIGINT DEFAULT 0,
            revenue NUMERIC DEFAULT 0,
            loaded_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY (metric_date, sku)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_sku_day_metrics_date ON ym_sku_day_metrics(metric_date);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS ym_ads_campaign_daily (
            campaign_id TEXT NOT NULL,
            stat_date DATE NOT NULL,
            impressions BIGINT DEFAULT 0,
            clicks BIGINT DEFAULT 0,
            spend NUMERIC DEFAULT 0,
            orders_cnt BIGINT DEFAULT 0,
            orders_amount NUMERIC DEFAULT 0,
            loaded_at TIMESTAMP DEFAULT now(),
            raw JSONB,
            PRIMARY KEY (campaign_id, stat_date)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_ym_ads_campaign_daily_date ON ym_ads_campaign_daily(stat_date);")


def create_wildberries_tables() -> None:
    execute_query(
        """
        CREATE TABLE IF NOT EXISTS wb_orders (
            order_id TEXT PRIMARY KEY,
            order_uid TEXT,
            order_date TIMESTAMP,
            status TEXT,
            warehouse_name TEXT,
            article TEXT,
            nm_id BIGINT,
            price NUMERIC,
            sale_price NUMERIC,
            updated_at TIMESTAMP DEFAULT now(),
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_orders_order_date ON wb_orders(order_date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_orders_status ON wb_orders(status);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS wb_order_items (
            id BIGSERIAL PRIMARY KEY,
            order_id TEXT REFERENCES wb_orders(order_id) ON DELETE CASCADE,
            sku TEXT,
            article TEXT,
            quantity NUMERIC,
            price NUMERIC,
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_order_items_order_id ON wb_order_items(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_order_items_article ON wb_order_items(article);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS wb_finance_items (
            id BIGSERIAL PRIMARY KEY,
            order_id TEXT REFERENCES wb_orders(order_id) ON DELETE CASCADE,
            fee_type TEXT,
            amount NUMERIC,
            happened_at TIMESTAMP,
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_finance_items_order_id ON wb_finance_items(order_id);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS wb_stocks_current (
            sku TEXT,
            warehouse_name TEXT,
            quantity NUMERIC DEFAULT 0,
            in_way_to_client NUMERIC DEFAULT 0,
            in_way_from_client NUMERIC DEFAULT 0,
            updated_at TIMESTAMP DEFAULT now(),
            raw JSONB,
            PRIMARY KEY (sku, warehouse_name)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_stocks_current_updated_at ON wb_stocks_current(updated_at);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS wb_reviews (
            review_id TEXT PRIMARY KEY,
            nm_id BIGINT,
            rating NUMERIC,
            review_text TEXT,
            published_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now(),
            raw JSONB
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_reviews_published_at ON wb_reviews(published_at);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS wb_sku_day_metrics (
            metric_date DATE NOT NULL,
            sku TEXT NOT NULL,
            impressions BIGINT DEFAULT 0,
            views BIGINT DEFAULT 0,
            ordered_units BIGINT DEFAULT 0,
            revenue NUMERIC DEFAULT 0,
            loaded_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY (metric_date, sku)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_sku_day_metrics_date ON wb_sku_day_metrics(metric_date);")

    execute_query(
        """
        CREATE TABLE IF NOT EXISTS wb_ads_campaign_daily (
            campaign_id TEXT NOT NULL,
            stat_date DATE NOT NULL,
            impressions BIGINT DEFAULT 0,
            clicks BIGINT DEFAULT 0,
            spend NUMERIC DEFAULT 0,
            orders_cnt BIGINT DEFAULT 0,
            orders_amount NUMERIC DEFAULT 0,
            loaded_at TIMESTAMP DEFAULT now(),
            raw JSONB,
            PRIMARY KEY (campaign_id, stat_date)
        );
        """
    )
    execute_query("CREATE INDEX IF NOT EXISTS idx_wb_ads_campaign_daily_date ON wb_ads_campaign_daily(stat_date);")


def create_marketplace_views() -> None:
    execute_query("DROP VIEW IF EXISTS public.marketplace_order_items_enriched;")
    execute_query("DROP VIEW IF EXISTS public.marketplace_data_freshness;")
    execute_query("DROP VIEW IF EXISTS public.marketplace_sku_day_metrics;")
    execute_query("DROP VIEW IF EXISTS public.marketplace_stocks_current;")
    execute_query("DROP VIEW IF EXISTS public.marketplace_ads_daily;")
    execute_query("DROP VIEW IF EXISTS public.marketplace_finance_items;")
    execute_query("DROP VIEW IF EXISTS public.marketplace_order_items;")
    execute_query("DROP VIEW IF EXISTS public.marketplace_orders;")

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_orders AS
        SELECT
            'ozon'::text AS marketplace,
            o.order_id::text AS order_id,
            o.order_date AS order_date,
            o.status AS status,
            o.revenue::numeric AS revenue,
            o.customer_id::text AS customer_key,
            o.fulfillment_type::text AS fulfillment_type,
            o.warehouse_name::text AS warehouse_name,
            o.delivery_city::text AS delivery_city,
            o.cluster_to::text AS delivery_cluster,
            o.promo_code::text AS promo_code,
            o.ozon_actions::text AS promo_actions,
            NULL::timestamp AS updated_at
        FROM public.orders o
        WHERE o.customer_id <> '47533921'

        UNION ALL

        SELECT
            'yandex_market'::text AS marketplace,
            o.order_id::text AS order_id,
            o.order_date AS order_date,
            o.status AS status,
            o.total_amount::numeric AS revenue,
            COALESCE(o.campaign_id::text, o.business_id::text) AS customer_key,
            NULL::text AS fulfillment_type,
            o.warehouse_id::text AS warehouse_name,
            NULL::text AS delivery_city,
            NULL::text AS delivery_cluster,
            NULL::text AS promo_code,
            NULL::text AS promo_actions,
            o.updated_at AS updated_at
        FROM public.ym_orders o

        UNION ALL

        SELECT
            'wildberries'::text AS marketplace,
            o.order_id::text AS order_id,
            o.order_date AS order_date,
            o.status AS status,
            COALESCE(o.sale_price, o.price)::numeric AS revenue,
            COALESCE(o.order_uid, o.article)::text AS customer_key,
            NULL::text AS fulfillment_type,
            o.warehouse_name::text AS warehouse_name,
            NULL::text AS delivery_city,
            NULL::text AS delivery_cluster,
            NULL::text AS promo_code,
            NULL::text AS promo_actions,
            o.updated_at AS updated_at
        FROM public.wb_orders o;
        """
    )

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_order_items AS
        SELECT
            'ozon'::text AS marketplace,
            oi.order_id::text AS order_id,
            o.order_date AS order_date,
            o.status AS status,
            oi.sku::text AS sku,
            p.name::text AS product_name,
            oi.quantity::numeric AS quantity,
            oi.revenue::numeric AS item_revenue,
            o.fulfillment_type::text AS fulfillment_type,
            o.warehouse_name::text AS warehouse_name,
            NULL::timestamp AS updated_at
        FROM public.order_items oi
        JOIN public.orders o ON o.order_id = oi.order_id
        LEFT JOIN public.products p ON p.sku = oi.sku
        WHERE o.customer_id <> '47533921'

        UNION ALL

        SELECT
            'yandex_market'::text AS marketplace,
            oi.order_id::text AS order_id,
            o.order_date AS order_date,
            o.status AS status,
            COALESCE(oi.sku, oi.offer_id)::text AS sku,
            oi.name::text AS product_name,
            oi.quantity::numeric AS quantity,
            (oi.quantity * oi.price)::numeric AS item_revenue,
            NULL::text AS fulfillment_type,
            o.warehouse_id::text AS warehouse_name,
            o.updated_at AS updated_at
        FROM public.ym_order_items oi
        JOIN public.ym_orders o ON o.order_id = oi.order_id

        UNION ALL

        SELECT
            'wildberries'::text AS marketplace,
            oi.order_id::text AS order_id,
            o.order_date AS order_date,
            o.status AS status,
            COALESCE(oi.sku, oi.article)::text AS sku,
            oi.article::text AS product_name,
            COALESCE(oi.quantity, 1)::numeric AS quantity,
            (COALESCE(oi.quantity, 1) * COALESCE(oi.price, o.sale_price, o.price))::numeric AS item_revenue,
            NULL::text AS fulfillment_type,
            o.warehouse_name::text AS warehouse_name,
            o.updated_at AS updated_at
        FROM public.wb_order_items oi
        JOIN public.wb_orders o ON o.order_id = oi.order_id;
        """
    )

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_order_items_enriched AS
        SELECT
            src.marketplace,
            src.order_id,
            src.order_date,
            src.status,
            src.sku,
            src.product_name,
            src.quantity,
            src.item_revenue,
            src.fulfillment_type,
            src.warehouse_name,
            src.updated_at,
            src.offer_id,
            src.article,
            src.nm_id,
            cp.canonical_product_id,
            COALESCE(cp.canonical_key, src.seed_canonical_key) AS canonical_key,
            COALESCE(cp.canonical_name, src.seed_canonical_name, src.product_name) AS canonical_name,
            COALESCE(cp.flavor, src.seed_flavor) AS flavor,
            COALESCE(cp.grams, src.seed_grams) AS grams,
            src.mapping_key_type,
            src.mapping_key
        FROM (
            SELECT
                'ozon'::text AS marketplace,
                oi.order_id::text AS order_id,
                o.order_date AS order_date,
                o.status AS status,
                oi.sku::text AS sku,
                p.name::text AS product_name,
                oi.quantity::numeric AS quantity,
                oi.revenue::numeric AS item_revenue,
                o.fulfillment_type::text AS fulfillment_type,
                o.warehouse_name::text AS warehouse_name,
                NULL::timestamp AS updated_at,
                NULL::text AS offer_id,
                NULL::text AS article,
                NULL::bigint AS nm_id,
                map_ozon.canonical_product_id,
                CONCAT('ozon-sku-', oi.sku::text) AS seed_canonical_key,
                p.name::text AS seed_canonical_name,
                p.flavor::text AS seed_flavor,
                p.grams::int AS seed_grams,
                map_ozon.external_key_type AS mapping_key_type,
                map_ozon.external_key AS mapping_key
            FROM public.order_items oi
            JOIN public.orders o ON o.order_id = oi.order_id
            LEFT JOIN public.products p ON p.sku = oi.sku
            LEFT JOIN public.marketplace_product_mapping map_ozon
              ON map_ozon.marketplace = 'ozon'
             AND map_ozon.external_key_type = 'sku'
             AND map_ozon.external_key = oi.sku::text
            WHERE o.customer_id <> '47533921'

            UNION ALL

            SELECT
                'yandex_market'::text AS marketplace,
                oi.order_id::text AS order_id,
                o.order_date AS order_date,
                o.status AS status,
                COALESCE(oi.sku, oi.offer_id)::text AS sku,
                oi.name::text AS product_name,
                oi.quantity::numeric AS quantity,
                (oi.quantity * oi.price)::numeric AS item_revenue,
                NULL::text AS fulfillment_type,
                o.warehouse_id::text AS warehouse_name,
                o.updated_at AS updated_at,
                oi.offer_id::text AS offer_id,
                NULL::text AS article,
                NULL::bigint AS nm_id,
                COALESCE(map_ym_offer.canonical_product_id, map_ym_sku.canonical_product_id) AS canonical_product_id,
                NULL::text AS seed_canonical_key,
                NULL::text AS seed_canonical_name,
                NULL::text AS seed_flavor,
                NULL::int AS seed_grams,
                COALESCE(map_ym_offer.external_key_type, map_ym_sku.external_key_type) AS mapping_key_type,
                COALESCE(map_ym_offer.external_key, map_ym_sku.external_key) AS mapping_key
            FROM public.ym_order_items oi
            JOIN public.ym_orders o ON o.order_id = oi.order_id
            LEFT JOIN public.marketplace_product_mapping map_ym_offer
              ON map_ym_offer.marketplace = 'yandex_market'
             AND map_ym_offer.external_key_type = 'offer_id'
             AND map_ym_offer.external_key = oi.offer_id::text
            LEFT JOIN public.marketplace_product_mapping map_ym_sku
              ON map_ym_sku.marketplace = 'yandex_market'
             AND map_ym_sku.external_key_type = 'sku'
             AND map_ym_sku.external_key = oi.sku::text

            UNION ALL

            SELECT
                'wildberries'::text AS marketplace,
                oi.order_id::text AS order_id,
                o.order_date AS order_date,
                o.status AS status,
                COALESCE(oi.sku, oi.article)::text AS sku,
                oi.article::text AS product_name,
                COALESCE(oi.quantity, 1)::numeric AS quantity,
                (COALESCE(oi.quantity, 1) * COALESCE(oi.price, o.sale_price, o.price))::numeric AS item_revenue,
                NULL::text AS fulfillment_type,
                o.warehouse_name::text AS warehouse_name,
                o.updated_at AS updated_at,
                NULL::text AS offer_id,
                oi.article::text AS article,
                o.nm_id AS nm_id,
                COALESCE(map_wb_nmid.canonical_product_id, map_wb_article.canonical_product_id, map_wb_sku.canonical_product_id) AS canonical_product_id,
                NULL::text AS seed_canonical_key,
                NULL::text AS seed_canonical_name,
                NULL::text AS seed_flavor,
                NULL::int AS seed_grams,
                COALESCE(map_wb_nmid.external_key_type, map_wb_article.external_key_type, map_wb_sku.external_key_type) AS mapping_key_type,
                COALESCE(map_wb_nmid.external_key, map_wb_article.external_key, map_wb_sku.external_key) AS mapping_key
            FROM public.wb_order_items oi
            JOIN public.wb_orders o ON o.order_id = oi.order_id
            LEFT JOIN public.marketplace_product_mapping map_wb_nmid
              ON map_wb_nmid.marketplace = 'wildberries'
             AND map_wb_nmid.external_key_type = 'nm_id'
             AND map_wb_nmid.external_key = o.nm_id::text
            LEFT JOIN public.marketplace_product_mapping map_wb_article
              ON map_wb_article.marketplace = 'wildberries'
             AND map_wb_article.external_key_type = 'article'
             AND map_wb_article.external_key = oi.article::text
            LEFT JOIN public.marketplace_product_mapping map_wb_sku
              ON map_wb_sku.marketplace = 'wildberries'
             AND map_wb_sku.external_key_type = 'sku'
             AND map_wb_sku.external_key = oi.sku::text
        ) src
        LEFT JOIN public.canonical_products cp
          ON cp.canonical_product_id = src.canonical_product_id;
        """
    )

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_finance_items AS
        SELECT
            'ozon'::text AS marketplace,
            fi.order_id::text AS order_id,
            o.order_date AS happened_at,
            fi.fee_group::text AS fee_group,
            fi.fee_name::text AS fee_name,
            ABS(fi.amount)::numeric AS amount,
            fi.source::text AS fee_source
        FROM public.order_fee_items fi
        LEFT JOIN public.orders o ON o.order_id = fi.order_id

        UNION ALL

        SELECT
            'yandex_market'::text AS marketplace,
            fi.order_id::text AS order_id,
            fi.happened_at AS happened_at,
            NULL::text AS fee_group,
            fi.fee_type::text AS fee_name,
            ABS(fi.amount)::numeric AS amount,
            'finance_items'::text AS fee_source
        FROM public.ym_finance_items fi

        UNION ALL

        SELECT
            'wildberries'::text AS marketplace,
            fi.order_id::text AS order_id,
            fi.happened_at AS happened_at,
            NULL::text AS fee_group,
            fi.fee_type::text AS fee_name,
            ABS(fi.amount)::numeric AS amount,
            'finance_items'::text AS fee_source
        FROM public.wb_finance_items fi;
        """
    )

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_ads_daily AS
        SELECT
            'ozon'::text AS marketplace,
            pcd.stat_date::date AS stat_date,
            pcd.campaign_id::text AS campaign_id,
            pcd.campaign_title::text AS campaign_name,
            COALESCE(pcd.impressions, 0)::bigint AS impressions,
            COALESCE(pcd.clicks, 0)::bigint AS clicks,
            COALESCE(pcd.spend, 0)::numeric AS spend,
            COALESCE(pcd.orders_cnt, 0)::bigint AS orders_cnt,
            COALESCE(pcd.orders_amount, 0)::numeric AS orders_amount
        FROM public.performance_campaign_daily pcd

        UNION ALL

        SELECT
            'yandex_market'::text AS marketplace,
            pcd.stat_date::date AS stat_date,
            pcd.campaign_id::text AS campaign_id,
            pcd.campaign_id::text AS campaign_name,
            COALESCE(pcd.impressions, 0)::bigint AS impressions,
            COALESCE(pcd.clicks, 0)::bigint AS clicks,
            COALESCE(pcd.spend, 0)::numeric AS spend,
            COALESCE(pcd.orders_cnt, 0)::bigint AS orders_cnt,
            COALESCE(pcd.orders_amount, 0)::numeric AS orders_amount
        FROM public.ym_ads_campaign_daily pcd

        UNION ALL

        SELECT
            'wildberries'::text AS marketplace,
            pcd.stat_date::date AS stat_date,
            pcd.campaign_id::text AS campaign_id,
            pcd.campaign_id::text AS campaign_name,
            COALESCE(pcd.impressions, 0)::bigint AS impressions,
            COALESCE(pcd.clicks, 0)::bigint AS clicks,
            COALESCE(pcd.spend, 0)::numeric AS spend,
            COALESCE(pcd.orders_cnt, 0)::bigint AS orders_cnt,
            COALESCE(pcd.orders_amount, 0)::numeric AS orders_amount
        FROM public.wb_ads_campaign_daily pcd;
        """
    )

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_stocks_current AS
        SELECT
            'ozon'::text AS marketplace,
            sc.sku::text AS sku,
            sc.warehouse_name::text AS warehouse_name,
            COALESCE(sc.free_to_sell, 0)::numeric AS quantity_available,
            COALESCE(sc.reserved, 0)::numeric AS quantity_reserved,
            NULL::numeric AS quantity_in_transit,
            sc.updated_at AS updated_at
        FROM public.stocks_current sc

        UNION ALL

        SELECT
            'yandex_market'::text AS marketplace,
            sc.sku::text AS sku,
            sc.warehouse_name::text AS warehouse_name,
            COALESCE(sc.fit, 0)::numeric AS quantity_available,
            COALESCE(sc.freeze_qty, 0)::numeric AS quantity_reserved,
            NULL::numeric AS quantity_in_transit,
            sc.updated_at AS updated_at
        FROM public.ym_stocks_current sc

        UNION ALL

        SELECT
            'wildberries'::text AS marketplace,
            sc.sku::text AS sku,
            sc.warehouse_name::text AS warehouse_name,
            COALESCE(sc.quantity, 0)::numeric AS quantity_available,
            NULL::numeric AS quantity_reserved,
            COALESCE(sc.in_way_to_client, 0)::numeric + COALESCE(sc.in_way_from_client, 0)::numeric AS quantity_in_transit,
            sc.updated_at AS updated_at
        FROM public.wb_stocks_current sc;
        """
    )

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_sku_day_metrics AS
        SELECT
            'ozon'::text AS marketplace,
            m.date::date AS metric_date,
            m.sku::text AS sku,
            COALESCE(m.impressions, 0)::bigint AS impressions,
            COALESCE(m.views, 0)::bigint AS views,
            COALESCE(m.cart_adds, 0)::bigint AS cart_adds,
            COALESCE(m.ordered_units, 0)::bigint AS ordered_units,
            COALESCE(m.revenue, 0)::numeric AS revenue,
            m.loaded_at::timestamp AS loaded_at
        FROM public.ozon_sku_day_metrics m

        UNION ALL

        SELECT
            'yandex_market'::text AS marketplace,
            m.metric_date::date AS metric_date,
            m.sku::text AS sku,
            COALESCE(m.impressions, 0)::bigint AS impressions,
            COALESCE(m.views, 0)::bigint AS views,
            0::bigint AS cart_adds,
            COALESCE(m.ordered_units, 0)::bigint AS ordered_units,
            COALESCE(m.revenue, 0)::numeric AS revenue,
            m.loaded_at::timestamp AS loaded_at
        FROM public.ym_sku_day_metrics m

        UNION ALL

        SELECT
            'wildberries'::text AS marketplace,
            m.metric_date::date AS metric_date,
            m.sku::text AS sku,
            COALESCE(m.impressions, 0)::bigint AS impressions,
            COALESCE(m.views, 0)::bigint AS views,
            0::bigint AS cart_adds,
            COALESCE(m.ordered_units, 0)::bigint AS ordered_units,
            COALESCE(m.revenue, 0)::numeric AS revenue,
            m.loaded_at::timestamp AS loaded_at
        FROM public.wb_sku_day_metrics m;
        """
    )

    execute_query(
        """
        CREATE OR REPLACE VIEW public.marketplace_data_freshness AS
        WITH marketplaces AS (
            SELECT 'ozon'::text AS marketplace
            UNION ALL SELECT 'yandex_market'::text
            UNION ALL SELECT 'wildberries'::text
        ),
        orders_agg AS (
            SELECT marketplace, MAX(order_date) AS last_order_at, COUNT(*) AS orders_rows
            FROM public.marketplace_orders
            GROUP BY marketplace
        ),
        finance_agg AS (
            SELECT marketplace, MAX(happened_at) AS last_finance_at, COUNT(*) AS finance_rows
            FROM public.marketplace_finance_items
            GROUP BY marketplace
        ),
        ads_agg AS (
            SELECT marketplace, MAX(stat_date)::timestamp AS last_ads_at, COUNT(*) AS ads_rows
            FROM public.marketplace_ads_daily
            GROUP BY marketplace
        ),
        stocks_agg AS (
            SELECT marketplace, MAX(updated_at) AS last_stock_at, COUNT(*) AS stock_rows
            FROM public.marketplace_stocks_current
            GROUP BY marketplace
        ),
        metrics_agg AS (
            SELECT marketplace, MAX(metric_date)::timestamp AS last_metric_at, COUNT(*) AS metric_rows
            FROM public.marketplace_sku_day_metrics
            GROUP BY marketplace
        )
        SELECT
            m.marketplace,
            o.last_order_at,
            f.last_finance_at,
            a.last_ads_at,
            s.last_stock_at,
            k.last_metric_at,
            COALESCE(o.orders_rows, 0) AS orders_rows,
            COALESCE(f.finance_rows, 0) AS finance_rows,
            COALESCE(a.ads_rows, 0) AS ads_rows,
            COALESCE(s.stock_rows, 0) AS stock_rows,
            COALESCE(k.metric_rows, 0) AS metric_rows
        FROM marketplaces m
        LEFT JOIN orders_agg o ON o.marketplace = m.marketplace
        LEFT JOIN finance_agg f ON f.marketplace = m.marketplace
        LEFT JOIN ads_agg a ON a.marketplace = m.marketplace
        LEFT JOIN stocks_agg s ON s.marketplace = m.marketplace
        LEFT JOIN metrics_agg k ON k.marketplace = m.marketplace;
        """
    )

# -----------------------------
# Runner
# -----------------------------

def run() -> None:
    print("[migrations] customers...")
    create_customers_table()

    print("[migrations] orders...")
    create_orders_table()

    print("[migrations] products...")
    create_products_table()

    print("[migrations] canonical product bridge...")
    create_canonical_products_tables()
    seed_ozon_canonical_products()

    print("[migrations] order_items...")
    create_order_items_table()

    print("[migrations] order_fee_items...")
    create_order_fee_items_table()

    print("[migrations] ads_campaigns...")
    create_ads_campaigns_table()

    print("[migrations] perf_campaigns...")
    create_perf_campaigns_table()

    print("[migrations] performance_campaign_daily...")
    create_performance_campaign_daily_table()

    print("[migrations] performance_order_attribution...")
    create_performance_order_attribution_table()

    print("[migrations] finance_period_costs...")
    create_finance_period_costs_table()

    print("[migrations] ozon_reviews...")
    create_reviews_table()

    print("[migrations] ozon_sku_day_metrics...")
    create_ozon_sku_day_metrics_table()

    print("[migrations] ozon_sku_day_metrics view...")
    create_vw_ozon_sku_day_funnel()

    print("[migrations] stocks_current...")
    create_stocks_current_table()

    print("[migrations] ym_* tables...")
    create_yandex_market_tables()

    print("[migrations] wb_* tables...")
    create_wildberries_tables()

    print("[migrations] marketplace views...")
    create_marketplace_views()

    print("[migrations] OK ✅")

if __name__ == "__main__":
    run()