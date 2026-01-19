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
            is_first_order BOOLEAN,
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
    execute_query("DROP INDEX IF EXISTS ux_order_fee_items_uid;")
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

    print("[migrations] OK ✅")


if __name__ == "__main__":
    run()