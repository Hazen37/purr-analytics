# src/migrations.py

"""
Скрипт для создания таблиц в базе данных PostgreSQL.

Его задача:
- Один раз запустить и создать нужные таблицы.
- Если таблицы уже существуют — ничего страшного, мы используем CREATE TABLE IF NOT EXISTS.

Это упрощённый вариант миграций.
В реальных проектах часто используют Alembic и т.п.,
но для нашей аналитики достаточно одного скрипта.
"""

from .db import execute_query


def create_customers_table():
    """
    Создаёт таблицу customers, если она ещё не существует.

    В этой таблице мы храним агрегированную информацию по клиенту:
    - customer_id       — наш идентификатор клиента (первая часть ID заказа на Ozon)
    - first_order_date  — дата первого заказа
    - last_order_date   — дата последнего заказа
    - orders_count      — количество заказов клиента
    - total_revenue     — общая выручка от клиента
    """
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
    Создаёт таблицу orders, если она ещё не существует.

    Здесь храним каждый заказ отдельно:
    - order_id      — уникальный ID заказа/отправления
    - customer_id   — идентификатор клиента (ссылка на customers.customer_id)
    - order_date    — дата и время заказа
    - revenue       — сумма заказа (выручка)
    - campaign      — строка с идентификатором/названием рекламной кампании (если получится сопоставить)
    - is_first_order — флаг: первый ли это заказ клиента (True/False)

    Важно:
    - FOREIGN KEY делает логическую связь с таблицей customers,
      но мы будем аккуратно следить за порядком вставки данных.
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

def add_ozon_payout_columns_to_orders():
    query = """
    ALTER TABLE orders
      ADD COLUMN IF NOT EXISTS ozon_fees_total NUMERIC,
      ADD COLUMN IF NOT EXISTS ozon_payout NUMERIC;
    """
    execute_query(query)

def create_ads_campaigns_table():
    """
    Создаёт таблицу ads_campaigns, если она ещё не существует.

    Здесь храним агрегированные показатели по рекламным кампаниям.

    Поля:
    - campaign_id   — уникальный идентификатор кампании (строка)
    - name          — понятное имя кампании (из кабинета Ozon Ads)
    - date          — дата, за которую считаются метрики (например, отчёт по дням)
    - clicks        — количество кликов
    - impressions   — количество показов
    - spend         — расходы (в рублях)
    - ozon_orders   — количество заказов, которые Ozon приписал этой кампании
    - ozon_revenue  — выручка по этим заказам

    Мы будем загружать сюда данные из Performance API.
    """
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

def create_products_table():
    """
    Таблица с товарами (каталог).

    sku    — идентификатор товара в OZON (обычно number, но храним как BIGINT или TEXT).
    name   — название товара.
    flavor — вкус корма (курица, говядина, утка...).
    grams  — граммовка упаковки (например, 400, 800 и т.п.).

    flavor и grams мы будем заполнять из словаря PRODUCT_CATALOG.
    """
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
    """
    Строки заказа (позиции).

    Здесь каждая строка = один товар в заказе:
    - order_id — внешний ключ на orders.order_id
    - sku      — внешний ключ на products.sku
    - quantity — количество штук
    - price    — цена за единицу
    - revenue  — выручка по строке (price * quantity)

    На уровне метрик мы будем фильтровать по flavor/grams через JOIN
    orders -> order_items -> products.
    """
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

def create_order_fee_items_table():
    """
    Детализация удержаний/начислений по заказу.
    Пока заполняем минимум: комиссия за продажу из financial_data.products[].
    Позже добавим логистику/эквайринг/рекламу из отчётов.
    """
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


def run_migrations():
    """
    Основная функция, которая вызывает создание всех таблиц.

    Её мы будем вызывать из блока if __name__ == "__main__",
    чтобы по запуску скрипта одним действием создать всю схему.
    """
    print("[migrations] Создаём таблицу customers...")
    create_customers_table()
    print("[migrations] Создаём таблицу orders...")
    create_orders_table()
    print("[migrations] Создаём таблицу products...")
    create_products_table()
    print("[migrations] Создаём таблицу order_items...")
    create_order_items_table()
    print("[migrations] Создаём таблицу order_fee_items...")
    create_order_fee_items_table()
    print("[migrations] Создаём таблицу ads_campaigns...")
    print("[migrations] Добавляем ozon_fees_total / ozon_payout в orders...")
    add_ozon_payout_columns_to_orders()
    create_ads_campaigns_table()
    print("[migrations] Готово!")


if __name__ == "__main__":
    # Если запускать этот файл напрямую как модуль:
    # python -m src.migrations
    run_migrations()