# src/etl.py

"""
ETL-скрипты для работы с данными Ozon.

Задачи этого модуля:
- Забрать заказы (отправления) из Ozon Seller API (FBO).
- Преобразовать их в удобный формат.
- Сохранить в базу данных (таблица orders).
- Пересчитать агрегаты по клиентам (таблица customers)
  и флаг is_first_order для заказов.

ETL = Extract (достать) → Transform (преобразовать) → Load (загрузить).
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Optional

from .ozon_seller_api import get_default_seller_client
from .utils import extract_customer_id
from .db import execute_query

from .product_catalog import PRODUCT_CATALOG

from decimal import Decimal


# def extract_fee_items_from_posting(posting: dict) -> list[dict]:
#     """
#     Пытаемся вытащить удержания/начисления из posting["financial_data"].
#     Структура может отличаться, поэтому здесь аккуратная логика.
#     """
#     fd = posting.get("financial_data") or {}
#     items: list[dict] = []

#     # Популярный паттерн: fd может содержать блоки услуг/комиссий в виде списков словарей.
#     # Мы пройдёмся по всем спискам словарей и попробуем вытащить amount/price и name/type.
#     for k, v in fd.items():
#         if isinstance(v, list) and v and isinstance(v[0], dict):
#             for row in v:
#                 # возможные названия полей в разных форматах
#                 name = row.get("name") or row.get("type") or row.get("service_name") or k
#                 amount = row.get("amount") or row.get("price") or row.get("value")
#                 percent = row.get("percent") or row.get("rate")

#                 if amount is None:
#                     continue

#                 items.append({
#                     "fee_group": k,       # пока используем ключ блока как группу
#                     "fee_name": str(name),
#                     "amount": _parse_decimal(amount),
#                     "percent": _parse_decimal(percent) if percent is not None else None,
#                     "source": "posting_financial",
#                 })

#     return items

def _parse_decimal(value: Any) -> Decimal:
    """
    Вспомогательная функция: аккуратно привести значение к Decimal.

    Почему не float?
    - Цены и деньги лучше считать в Decimal, без ошибок округления.

    Ozon часто отдаёт цены строками, например "123.45",
    поэтому:
    - если пришло str, создаём Decimal из строки;
    - если пришло уже число, тоже оборачиваем в Decimal;
    - если None или пусто — считаем 0.
    """
    if value is None:
        return Decimal("0")

    if isinstance(value, Decimal):
        return value

    # Приводим к строке, чтобы Decimal не страдал от float'овской неточности
    return Decimal(str(value))


def calculate_order_revenue(posting: Dict[str, Any]) -> Decimal:
    """
    Рассчитать выручку по заказу (отправлению) на основе списка товаров.

    Варианты:
    - Можно использовать financial_data (там точнее, с учётом скидок и т.п.).
    - Для начала возьмём простой вариант: сумма price * quantity по products.

    Структура posting["products"] для FBO обычно такая:
    [
      {
        "sku": ...,
        "name": ...,
        "quantity": 1,
        "price": "123.45",
        ...
      },
      ...
    ]

    Возвращает:
    - Decimal с общей суммой по заказу.
    """
    products = posting.get("products", [])
    total = Decimal("0")

    for item in products:
        price = _parse_decimal(item.get("price"))
        quantity = item.get("quantity") or 0
        try:
            qty_dec = Decimal(str(quantity))
        except Exception:
            qty_dec = Decimal("0")

        total += price * qty_dec

    return total

def sync_order_items_and_products_from_posting(posting: Dict[str, Any]):
    """
    Сохранить строки заказа (order_items) и товары (products) для одного отправления.

    Логика:
    - Берём posting["products"] — список товаров в отправлении.
    - Для каждого товара:
        - читаем sku, name, quantity, price
        - ищем в PRODUCT_CATALOG вкус и граммовку
        - UPSERT в products
        - вставляем строку в order_items

    Перед вставкой строк по заказу:
    - удаляем старые строки из order_items по этому order_id,
      чтобы при повторном прогоне ETL не плодить дубликаты.
    """
    posting_number = posting.get("posting_number")
    if not posting_number:
        print("[etl] Невозможно синхронизировать order_items: нет posting_number")
        return

    products = posting.get("products", [])

    # Сначала удалим старые строки по этому заказу
    delete_query = "DELETE FROM order_items WHERE order_id = %s;"
    execute_query(delete_query, (posting_number,))

    for item in products:
        sku = item.get("sku")
        name = item.get("name")
        quantity = item.get("quantity") or 0
        price = _parse_decimal(item.get("price"))

        # Выручка по строке = price * quantity
        try:
            qty_dec = _parse_decimal(quantity)
        except Exception:
            qty_dec = _parse_decimal(0)

        line_revenue = price * qty_dec

        # Ищем вкус и граммовку в словаре PRODUCT_CATALOG
        attrs = PRODUCT_CATALOG.get(sku, {})
        flavor = attrs.get("flavor")
        grams = attrs.get("grams")

        # 1. UPSERT в products
        product_query = """
        INSERT INTO products (sku, name, flavor, grams)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sku) DO UPDATE
        SET
            name   = EXCLUDED.name,
            flavor = EXCLUDED.flavor,
            grams  = EXCLUDED.grams;
        """
        execute_query(product_query, (sku, name, flavor, grams))

        # 2. Вставляем строку заказа в order_items
        item_query = """
        INSERT INTO order_items (order_id, sku, quantity, price, revenue)
        VALUES (%s, %s, %s, %s, %s);
        """
        execute_query(item_query, (posting_number, sku, quantity, price, line_revenue))

def upsert_order_from_posting(posting: Dict[str, Any]):
    """
    Сохранить один заказ (отправление) в таблицу orders.

    Шаги:
    1) Вытащить posting_number и customer_id.
    2) Убедиться, что клиент уже есть в таблице customers (если нет — создать "заглушку").
    3) Вставить/обновить запись в таблице orders.
    """
    posting_number = posting.get("posting_number")
    if not posting_number:
        print("[etl] Пропускаем отправление без posting_number:", posting)
        return

    customer_id = extract_customer_id(posting_number)

    # 1. Сначала гарантируем, что клиент есть в таблице customers.
    #    Мы вставляем только customer_id, остальные поля (даты, выручка)
    #    потом аккуратно пересчитаем в recalc_customers_aggregates().
    create_customer_query = """
    INSERT INTO customers (customer_id)
    VALUES (%s)
    ON CONFLICT (customer_id) DO NOTHING;
    """
    execute_query(create_customer_query, (customer_id,))

    # 2. Теперь можно спокойно вставлять заказ — внешний ключ уже не будет ругаться.
    in_process_at = posting.get("in_process_at")
    if in_process_at is None:
        order_date = None
    else:
        order_date = in_process_at  # PostgreSQL сам приведёт строку ISO к TIMESTAMP
    status = posting.get("status")

    revenue = calculate_order_revenue(posting)

    ozon_payout, ozon_fees_total, fee_items = extract_ozon_finance_from_posting(posting)

    campaign = None  # позже сюда можно будет писать ID/имя рекламной кампании

    order_query = """
    INSERT INTO orders (
        order_id, customer_id, order_date, status,
        revenue, ozon_fees_total, ozon_payout,
        campaign, is_first_order
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)
    ON CONFLICT (order_id) DO UPDATE
    SET
        customer_id     = EXCLUDED.customer_id,
        order_date      = EXCLUDED.order_date,
        status          = EXCLUDED.status,
        revenue         = EXCLUDED.revenue,
        ozon_fees_total = EXCLUDED.ozon_fees_total,
        ozon_payout     = EXCLUDED.ozon_payout,
        campaign        = EXCLUDED.campaign;
    """

    params = (
        posting_number,
        customer_id,
        order_date,
        status,
        revenue,
        ozon_fees_total,
        ozon_payout,
        campaign,
    )

    execute_query(order_query, params)

    # после вставки заказа — сохраняем детализацию удержаний
    sync_order_fee_items(posting_number, fee_items)

    sync_order_items_and_products_from_posting(posting)


def recalc_customers_aggregates():
    """
    Пересчитать агрегированную таблицу customers на основе таблицы orders.

    Логика:
    - для каждого customer_id считаем:
        - first_order_date  — MIN(order_date)
        - last_order_date   — MAX(order_date)
        - orders_count      — COUNT(*)
        - total_revenue     — SUM(revenue)

    Используем INSERT ... SELECT ... ON CONFLICT, чтобы:
    - добавить новых клиентов,
    - обновить данные по уже существующим.
    """
    query = """
    INSERT INTO customers (customer_id, first_order_date, last_order_date, orders_count, total_revenue)
    SELECT
        customer_id,
        MIN(order_date)::date      AS first_order_date,
        MAX(order_date)::date      AS last_order_date,
        COUNT(*)                   AS orders_count,
        SUM(revenue)               AS total_revenue
    FROM orders
    GROUP BY customer_id
    ON CONFLICT (customer_id) DO UPDATE
    SET
        first_order_date = EXCLUDED.first_order_date,
        last_order_date  = EXCLUDED.last_order_date,
        orders_count     = EXCLUDED.orders_count,
        total_revenue    = EXCLUDED.total_revenue
    ;
    """
    execute_query(query)


def recalc_is_first_order_flags():
    """
    Обновить флаг is_first_order в таблице orders.

    Логика:
    - для каждого клиента находим минимальную дату заказа;
    - отмечаем заказы с этой датой как первые (is_first_order = TRUE),
      остальные — FALSE.

    Если у клиента несколько заказов в одну и ту же дату/время —
    все они будут помечены как первые. Это не критично для аналитики,
    поскольку чаще всего нас интересует сам факт "первый заказ был в такой-то день".
    """
    query = """
    WITH first_orders AS (
        SELECT
            customer_id,
            MIN(order_date) AS first_order_date
        FROM orders
        GROUP BY customer_id
    )
    UPDATE orders o
    SET is_first_order = (o.order_date = f.first_order_date)
    FROM first_orders f
    WHERE o.customer_id = f.customer_id;
    """
    execute_query(query)


def load_fbo_orders_for_period(date_from: datetime, date_to: datetime):
    """
    Основная ETL-функция:
    - забирает FBO-отправления за указанный период
    - сохраняет их в таблицу orders
    - пересчитывает customers и флаги is_first_order

    Этот метод можно вызывать:
    - вручную (для теста),
    - по расписанию (cron, планировщик задач),
    - или из другого кода (например, main.py).
    """
    client = get_default_seller_client()

    print(f"[etl] Загружаем FBO-отправления с {date_from} по {date_to}...")

    postings = client.get_postings_fbo(date_from=date_from, date_to=date_to, limit=100)

    print(f"[etl] Получено отправлений: {len(postings)}")

    # import json
    # ВРЕМЕННО: печатаем financial_data первого заказа и сразу выходим
    # if postings:
    #     fd = postings[0].get("financial_data")
    #     print("=== FINANCIAL_DATA SAMPLE ===")
    #     print(json.dumps(fd, ensure_ascii=False, indent=2))
    #     print("=== END FINANCIAL_DATA SAMPLE ===")
    #     return

    for p in postings:
        upsert_order_from_posting(p)

    print("[etl] Заказы записаны в таблицу orders. Пересчитываем агрегаты...")

    recalc_customers_aggregates()
    recalc_is_first_order_flags()
    # ✅ пересчёт денег и разложение комиссий по группам
    recalc_orders_finance()
    recalc_orders_fees_breakdown()


    print("[etl] Агрегаты по клиентам и флаги is_first_order обновлены.")


def load_fbo_orders_last_n_days(days: int = 30):
    """
    Удобный враппер: загрузить заказы за последние N дней.

    Пример использования:
        load_fbo_orders_last_n_days(30)

    Это можно повесить на ежедневный запуск:
    - каждый день подтягиваем последние 30 дней, уплотняя данные.
    """
    date_to = datetime.utcnow()
    date_from = date_to - timedelta(days=days)
    load_fbo_orders_for_period(date_from=date_from, date_to=date_to)

from decimal import Decimal

def extract_ozon_finance_from_posting(posting: dict) -> tuple[Decimal, Decimal, list[dict]]:
    """
    Возвращает:
    - ozon_payout: сколько можно вывести (сумма payout по позициям)
    - ozon_fees_total: сумма удержаний Озона (пока только commission_amount)
    - fee_items: строки для order_fee_items
    """
    fd = posting.get("financial_data") or {}
    fin_products = fd.get("products") or []

    payout_total = Decimal("0")
    fees_total = Decimal("0")
    fee_items: list[dict] = []

    for p in fin_products:
        product_id = p.get("product_id")
        commission_amount = _parse_decimal(p.get("commission_amount"))  # обычно отрицательная
        commission_percent = p.get("commission_percent")
        payout = _parse_decimal(p.get("payout"))

        payout_total += payout
        fees_total += commission_amount

        # строка удержания — комиссия
        fee_items.append({
            "fee_group": "Вознаграждение Ozon",
            "fee_name": "Вознаграждение за продажу",
            "amount": commission_amount,                 # уже со знаком "-"
            "percent": _parse_decimal(commission_percent) if commission_percent is not None else None,
            "product_id": product_id,
            "source": "posting_financial",
        })

        # (опционально) можно сохранить скидки отдельной строкой, если появятся:
        discount_value = _parse_decimal(p.get("total_discount_value"))
        if discount_value != 0:
            # в API это обычно положительное число скидки, но по смыслу уменьшает продажи → делаем "-"
            fee_items.append({
                "fee_group": "Скидки",
                "fee_name": "Скидка",
                "amount": -discount_value,
                "percent": _parse_decimal(p.get("total_discount_percent")) if p.get("total_discount_percent") is not None else None,
                "product_id": product_id,
                "source": "posting_financial",
            })

    return payout_total, fees_total, fee_items


def sync_order_fee_items(order_id: str, fee_items: list[dict]):
    """
    Перезаписываем детализацию удержаний по заказу (чтобы ETL был идемпотентным).
    """
    execute_query(
        "DELETE FROM order_fee_items WHERE order_id = %s AND source = 'posting_financial';",
        (order_id,)
    )

    q = """
    INSERT INTO order_fee_items (order_id, fee_group, fee_name, amount, percent, product_id, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    for it in fee_items:
        execute_query(q, (
            order_id,
            it.get("fee_group"),
            it.get("fee_name"),
            it.get("amount"),
            it.get("percent"),
            it.get("product_id"),
            it.get("source", "posting_financial"),
        ))

def recalc_orders_fees_breakdown():
    """
    Разложение комиссий по заказу.
    Берём только строки, привязанные к order_id.

    Важно:
    - исключаем дублирующую комиссию из finance_api (если вдруг появится)
    """
    query = """
    WITH sums AS (
      SELECT
        order_id,

        -- total fees (всё вместе, кроме дублирующейся комиссии из finance_api)
        SUM(amount) AS fees_total,

        -- разрезы
        SUM(CASE WHEN fee_group='Услуги доставки' THEN amount ELSE 0 END)  AS delivery_fee,
        SUM(CASE WHEN fee_group='Услуги агентов' THEN amount ELSE 0 END)   AS acquiring_fee,
        SUM(CASE WHEN fee_group='Продвижение и реклама' THEN amount ELSE 0 END) AS ads_fee,

        -- отдельно комиссия и скидки
        SUM(CASE WHEN fee_group='Вознаграждение Ozon' THEN amount ELSE 0 END) AS sale_commission,
        SUM(CASE WHEN fee_group='Скидки' THEN amount ELSE 0 END) AS discount_fee,

        -- прочее: всё, что не попало в группы выше
        SUM(CASE
              WHEN fee_group NOT IN (
                'Услуги доставки',
                'Услуги агентов',
                'Продвижение и реклама',
                'Вознаграждение Ozon',
                'Скидки'
              )
              THEN amount ELSE 0
            END) AS other_fee_real

      FROM order_fee_items
      WHERE order_id IS NOT NULL
        AND NOT (source='finance_api' AND fee_group='Вознаграждение Ozon')
      GROUP BY order_id
    )
    UPDATE orders o
    SET
      ozon_fees_total        = COALESCE(s.fees_total, 0),
      ozon_delivery_fee      = COALESCE(s.delivery_fee, 0),
      ozon_acquiring_fee     = COALESCE(s.acquiring_fee, 0),
      ozon_ads_fee           = COALESCE(s.ads_fee, 0),

      ozon_sale_commission   = COALESCE(s.sale_commission, 0),
      ozon_discount          = COALESCE(s.discount_fee, 0),
      ozon_other_fee_real    = COALESCE(s.other_fee_real, 0),

      profit                 = COALESCE(o.revenue, 0) + COALESCE(s.fees_total, 0)

    FROM sums s
    WHERE o.order_id = s.order_id;
    """
    execute_query(query)

def recalc_orders_finance():
    """
    Пересчитываем финансы по данным order_fee_items.

    - sales_report = сумма по группе 'Продажи'
    - ozon_fees_total = сумма по всем группам КРОМЕ 'Продажи'
    - ozon_payout = sales_report + ozon_fees_total
    """
    # 1) sales_report
    execute_query("""
        UPDATE orders o
        SET sales_report = COALESCE(s.sales, 0)
        FROM (
            SELECT order_id, SUM(amount) AS sales
            FROM order_fee_items
            WHERE order_id IS NOT NULL
              AND fee_group = 'Продажи'
            GROUP BY order_id
        ) s
        WHERE o.order_id = s.order_id;
    """)

    # 2) все удержания/расходы (логистика, эквайринг, реклама, комиссия и т.д.)
    execute_query("""
        UPDATE orders o
        SET ozon_fees_total = COALESCE(f.fees, 0)
        FROM (
            SELECT order_id, SUM(amount) AS fees
            FROM order_fee_items
            WHERE order_id IS NOT NULL
            AND NOT (source='finance_api' AND fee_group='Вознаграждение Ozon')
            GROUP BY order_id
        ) f
        WHERE o.order_id = f.order_id;
    """)

    # 3) payout = sales_report + fees_total (если sales_report нет — fallback на revenue)
    execute_query("""
        UPDATE orders
        SET ozon_payout = COALESCE(revenue, 0) + COALESCE(ozon_fees_total, 0);
    """)


if __name__ == "__main__":
    # Если запускать этот модуль напрямую:
    # python -m src.etl
    # он подтянет заказы за последние 30 дней.
    from datetime import datetime

    load_fbo_orders_for_period(
        date_from=datetime(2025, 10, 1),
        date_to=datetime.now()
    )
