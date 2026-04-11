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

    print("[migrations] OK ✅")

if __name__ == "__main__":
    run()