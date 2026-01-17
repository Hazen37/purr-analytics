# src/migrations.py
"""
Упрощённые миграции: создаём таблицы и "догоняем" схему через ALTER TABLE ... ADD COLUMN IF NOT EXISTS.

Запуск:
  python -m src.migrations
"""

from .db import execute_query


# -----------------------------
# Core tables
# -----------------------------

def create_customers_table():
    query = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        first_order_date DATE,
        last_order_date DATE,
        orders_count INT,
        total_revenue NUMERIC
    );
    """
    execute_query(query)


def create_orders_table():
    """
    Базовая таблица заказов. Важно: часть колонок догоняется отдельными patch_* функциями.
    """
    query = """
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
    execute_query(query)

    # Базовые индексы для фильтраций/джойнов
    execute_query("CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);")


def create_products_table():
    query = """
    CREATE TABLE IF NOT EXISTS products (
        sku   BIGINT PRIMARY KEY,
        name  TEXT,
        flavor TEXT,
        grams  INT
    );
    """
    execute_query(query)


def create_order_items_table():
    query = """
    CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id TEXT REFERENCES orders(order_id) ON DELETE CASCADE,
        sku BIGINT REFERENCES products(sku),
        quantity INT,
        price NUMERIC,
        revenue NUMERIC
    );
    """
    execute_query(query)

    execute_query("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);")


def create_order_fee_items_table():
    query = """
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
    execute_query(query)

    execute_query("CREATE INDEX IF NOT EXISTS idx_order_fee_items_order_id ON order_fee_items(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_order_fee_items_group_name ON order_fee_items(fee_group, fee_name);")


def create_ads_campaigns_table():
    query = """
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
    execute_query(query)

    execute_query("CREATE INDEX IF NOT EXISTS idx_ads_campaigns_date ON ads_campaigns(date);")


# -----------------------------
# Additional / reporting tables
# -----------------------------

def create_performance_order_attribution_table():
    query = """
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
    execute_query(query)

    execute_query("CREATE INDEX IF NOT EXISTS idx_poa_order_id ON performance_order_attribution(order_id);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_poa_stat_date ON performance_order_attribution(stat_date);")
    execute_query("CREATE INDEX IF NOT EXISTS idx_poa_campaign_date ON performance_order_attribution(campaign_id, stat_date);")


def create_finance_period_costs_table():
    """
    Для отчётов/дашбордов: периодные расходы по дням/группам/статьям.
    """
    query = """
    CREATE TABLE IF NOT EXISTS finance_period_costs (
      cost_date DATE NOT NULL,
      fee_group TEXT NOT NULL,
      fee_name  TEXT NOT NULL,
      amount    NUMERIC NOT NULL DEFAULT 0,
      PRIMARY KEY (cost_date, fee_group, fee_name)
    );
    """
    execute_query(query)

    execute_query("CREATE INDEX IF NOT EXISTS idx_finance_period_costs_date ON finance_period_costs(cost_date);")


# -----------------------------
# Patch (ALTER TABLE) helpers
# -----------------------------

def patch_orders_core_columns():
    """
    Колонки, которые точно используются в src/etl.py (и уже всплывали ошибками).
    """
    query = """
    ALTER TABLE orders
      ADD COLUMN IF NOT EXISTS status TEXT,
      ADD COLUMN IF NOT EXISTS ozon_fees_total NUMERIC,
      ADD COLUMN IF NOT EXISTS ozon_payout NUMERIC,
      ADD COLUMN IF NOT EXISTS sales_report NUMERIC;
    """
    execute_query(query)


def patch_orders_fees_breakdown_columns():
    """
    Колонки для recalc_orders_fees_breakdown() (etl.py):
    - delivery_fee, acquiring_fee, ads_fee (и т.п.)
    """
    query = """
    ALTER TABLE orders
      ADD COLUMN IF NOT EXISTS ozon_delivery_fee NUMERIC,
      ADD COLUMN IF NOT EXISTS ozon_acquiring_fee NUMERIC,
      ADD COLUMN IF NOT EXISTS ozon_ads_fee NUMERIC;
    """
    execute_query(query)


def patch_orders_performance_columns():
    """
    Колонки для performance_orders_etl.py
    """
    query = """
    ALTER TABLE orders
      ADD COLUMN IF NOT EXISTS campaign_id TEXT,
      ADD COLUMN IF NOT EXISTS campaign_title TEXT,
      ADD COLUMN IF NOT EXISTS ozon_ads_attributed NUMERIC;
    """
    execute_query(query)


# -----------------------------
# Runner
# -----------------------------

def run_migrations():
    print("[migrations] customers...")
    create_customers_table()

    print("[migrations] orders...")
    create_orders_table()

    print("[migrations] patch orders (core columns)...")
    patch_orders_core_columns()

    print("[migrations] patch orders (fees breakdown)...")
    patch_orders_fees_breakdown_columns()

    print("[migrations] patch orders (performance)...")
    patch_orders_performance_columns()

    print("[migrations] products...")
    create_products_table()

    print("[migrations] order_items...")
    create_order_items_table()

    print("[migrations] order_fee_items...")
    create_order_fee_items_table()

    print("[migrations] ads_campaigns...")
    create_ads_campaigns_table()

    print("[migrations] performance_order_attribution...")
    create_performance_order_attribution_table()

    print("[migrations] finance_period_costs...")
    create_finance_period_costs_table()

    print("[migrations] OK ✅")


if __name__ == "__main__":
    run_migrations()
