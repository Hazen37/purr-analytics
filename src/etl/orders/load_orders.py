# src/etl/orders/load_orders.py
"""
ETL: загрузка заказов (FBO postings) из Ozon Seller API в Postgres.

Что делает:
- тянет postings за период
- upsert в orders
- перезаписывает order_items и products по каждому заказу
- перезаписывает order_fee_items (source='posting_financial') по каждому заказу
- пересчитывает customers и флаги is_first_order
- пересчитывает финансы по order_fee_items

Запуск (обычно через update_all):
  python -m src.cli.update_all 2025-10-01 2025-12-12
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from src.core.db import execute_query, fetch_one
from src.catalog.product_catalog import PRODUCT_CATALOG

# ВАЖНО:
# Я предполагаю, что у тебя в seller_api.py есть фабрика клиента и метод get_postings_fbo.
# Если имена отличаются — замени импорт/вызов ниже.
from src.ozon.seller_api import get_default_seller_client


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _dec(x: Any) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    # защищаемся от "1 234,56"
    s = str(x).replace(" ", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def extract_customer_id(posting_number: str) -> str:
    """
    Достаём customer_id из posting_number.
    Раньше у тебя это было в ozon_seller_api/utils.

    Если у тебя customer_id по-другому устроен — поменяй эту функцию.
    """
    # Частый формат: "<customer>-<something>" или "<customer>_<something>"
    s = (posting_number or "").strip()
    if not s:
        return "unknown"

    for sep in ("-", "_"):
        if sep in s:
            return s.split(sep, 1)[0]

    # fallback: весь posting_number как customer_id (лучше чем None)
    return s


# ---------------------------------------------------------------------
# Products + Order items
# ---------------------------------------------------------------------

def sync_order_items_and_products_from_posting(posting: Dict[str, Any]) -> None:
    """
    Перезаписывает order_items по заказу и upsert-ит products.
    """
    order_id = posting.get("posting_number")
    if not order_id:
        print("[orders] skip order_items sync: no posting_number")
        return

    products = posting.get("products") or []

    # идемпотентность: удаляем старые строки
    execute_query("DELETE FROM order_items WHERE order_id = %s;", (order_id,))

    for it in products:
        sku = it.get("sku")
        name = it.get("name")
        quantity = it.get("quantity") or 0
        price = _dec(it.get("price"))

        qty = _dec(quantity)
        line_revenue = price * qty

        attrs = PRODUCT_CATALOG.get(sku, {}) if sku is not None else {}
        flavor = attrs.get("flavor")
        grams = attrs.get("grams")

        # products upsert
        execute_query(
            """
            INSERT INTO products (sku, name, flavor, grams)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (sku) DO UPDATE
            SET name = EXCLUDED.name,
                flavor = EXCLUDED.flavor,
                grams = EXCLUDED.grams;
            """,
            (sku, name, flavor, grams),
        )

        # order_items insert
        execute_query(
            """
            INSERT INTO order_items (order_id, sku, quantity, price, revenue)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (order_id, sku, int(quantity) if str(quantity).isdigit() else quantity, price, line_revenue),
        )


# ---------------------------------------------------------------------
# Fees from posting.financial_data
# ---------------------------------------------------------------------

def extract_ozon_finance_from_posting(posting: Dict[str, Any]) -> Tuple[Decimal, Decimal, List[Dict[str, Any]]]:
    """
    Возвращает:
    - ozon_payout: сколько можно вывести (сумма payout по позициям)
    - ozon_fees_total: сумма удержаний Озона (комиссия/скидки) по posting.financial_data
    - fee_items: строки для order_fee_items (source='posting_financial')
    """
    fd = posting.get("financial_data") or {}
    fin_products = fd.get("products") or []

    payout_total = Decimal("0")
    fees_total = Decimal("0")
    fee_items: List[Dict[str, Any]] = []

    for p in fin_products:
        product_id = p.get("product_id")

        # В Ozon commission_amount часто отрицательная (удержание)
        commission_amount = _dec(p.get("commission_amount"))
        commission_percent = p.get("commission_percent")
        payout = _dec(p.get("payout"))

        payout_total += payout
        fees_total += commission_amount

        if commission_amount != 0:
            fee_items.append(
                {
                    "fee_group": "Вознаграждение Ozon",
                    "fee_name": "Вознаграждение за продажу",
                    "amount": commission_amount,
                    "percent": _dec(commission_percent) if commission_percent is not None else None,
                    "product_id": product_id,
                    "source": "posting_financial",
                }
            )

        # скидки (если есть)
        discount_value = _dec(p.get("total_discount_value"))
        if discount_value != 0:
            fee_items.append(
                {
                    "fee_group": "Скидки",
                    "fee_name": "Скидка",
                    "amount": -discount_value,  # скидка уменьшает выручку
                    "percent": _dec(p.get("total_discount_percent")) if p.get("total_discount_percent") is not None else None,
                    "product_id": product_id,
                    "source": "posting_financial",
                }
            )

    return payout_total, fees_total, fee_items


def sync_order_fee_items(order_id: str, fee_items: List[Dict[str, Any]]) -> None:
    """
    Перезаписываем детализацию удержаний по заказу (идемпотентность).
    Пишем только source='posting_financial'.
    """
    execute_query(
        "DELETE FROM order_fee_items WHERE order_id = %s AND source = 'posting_financial';",
        (order_id,),
    )

    q = """
    INSERT INTO order_fee_items (order_id, fee_group, fee_name, amount, percent, product_id, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    for it in fee_items:
        execute_query(
            q,
            (
                order_id,
                it.get("fee_group"),
                it.get("fee_name"),
                it.get("amount"),
                it.get("percent"),
                it.get("product_id"),
                it.get("source", "posting_financial"),
            ),
        )


# ---------------------------------------------------------------------
# Orders upsert
# ---------------------------------------------------------------------

def calculate_order_revenue(posting: Dict[str, Any]) -> Decimal:
    """
    Простой расчёт выручки: sum(price * quantity) по posting.products.
    """
    products = posting.get("products") or []
    total = Decimal("0")
    for it in products:
        total += _dec(it.get("price")) * _dec(it.get("quantity") or 0)
    return total


def upsert_order_from_posting(posting: Dict[str, Any]) -> None:
    order_id = posting.get("posting_number")
    if not order_id:
        print("[orders] skip posting without posting_number")
        return

    customer_id = extract_customer_id(str(order_id))

    # гарантируем customer
    execute_query(
        """
        INSERT INTO customers (customer_id)
        VALUES (%s)
        ON CONFLICT (customer_id) DO NOTHING;
        """,
        (customer_id,),
    )

    in_process_at = posting.get("in_process_at")
    order_date = in_process_at if in_process_at is not None else None
    status = posting.get("status")

    revenue = calculate_order_revenue(posting)
    ozon_payout, ozon_fees_total, fee_items = extract_ozon_finance_from_posting(posting)

    execute_query(
        """
        INSERT INTO orders (
            order_id, customer_id, order_date, status,
            revenue, ozon_fees_total, ozon_payout,
            campaign, is_first_order
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL)
        ON CONFLICT (order_id) DO UPDATE
        SET
            customer_id     = EXCLUDED.customer_id,
            order_date      = EXCLUDED.order_date,
            status          = EXCLUDED.status,
            revenue         = EXCLUDED.revenue,
            ozon_fees_total = EXCLUDED.ozon_fees_total,
            ozon_payout     = EXCLUDED.ozon_payout;
        """,
        (order_id, customer_id, order_date, status, revenue, ozon_fees_total, ozon_payout),
    )

    # детализация удержаний
    sync_order_fee_items(str(order_id), fee_items)

    # строки заказа + каталог
    sync_order_items_and_products_from_posting(posting)


# ---------------------------------------------------------------------
# Recalc агрегатов
# ---------------------------------------------------------------------

def recalc_customers_aggregates() -> None:
    execute_query(
        """
        INSERT INTO customers (customer_id, first_order_date, last_order_date, orders_count, total_revenue)
        SELECT
            customer_id,
            MIN(order_date)::date AS first_order_date,
            MAX(order_date)::date AS last_order_date,
            COUNT(*)              AS orders_count,
            SUM(revenue)          AS total_revenue
        FROM orders
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
        ON CONFLICT (customer_id) DO UPDATE
        SET
            first_order_date = EXCLUDED.first_order_date,
            last_order_date  = EXCLUDED.last_order_date,
            orders_count     = EXCLUDED.orders_count,
            total_revenue    = EXCLUDED.total_revenue;
        """
    )


def recalc_is_first_order_flags() -> None:
    execute_query(
        """
        WITH first_orders AS (
            SELECT customer_id, MIN(order_date) AS first_order_date
            FROM orders
            WHERE customer_id IS NOT NULL AND order_date IS NOT NULL
            GROUP BY customer_id
        )
        UPDATE orders o
        SET is_first_order = (o.order_date = f.first_order_date)
        FROM first_orders f
        WHERE o.customer_id = f.customer_id;
        """
    )


def recalc_orders_finance() -> None:
    """
    Пересчёт финансов по order_fee_items:
    - sales_report = сумма по fee_group='Продажи'
    - ozon_fees_total = сумма по всем (кроме дублирующей комиссии из finance_api)
    - ozon_payout = revenue + ozon_fees_total
    """
    execute_query(
        """
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
        """
    )

    execute_query(
        """
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
        """
    )

    execute_query(
        """
        UPDATE orders
        SET ozon_payout = COALESCE(revenue, 0) + COALESCE(ozon_fees_total, 0);
        """
    )


def recalc_orders_fees_breakdown() -> None:
    """
    Разложение комиссий по группам и profit.
    """
    execute_query(
        """
        WITH sums AS (
          SELECT
            order_id,

            SUM(amount) AS fees_total,

            SUM(CASE WHEN fee_group='Услуги доставки' THEN amount ELSE 0 END)  AS delivery_fee,
            SUM(CASE WHEN fee_group='Услуги агентов' THEN amount ELSE 0 END)   AS acquiring_fee,
            SUM(CASE WHEN fee_group='Продвижение и реклама' THEN amount ELSE 0 END) AS ads_fee,

            SUM(CASE WHEN fee_group='Вознаграждение Ozon' THEN amount ELSE 0 END) AS sale_commission,
            SUM(CASE WHEN fee_group='Скидки' THEN amount ELSE 0 END) AS discount_fee,

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
    )


# ---------------------------------------------------------------------
# Missing marking (optional)
# ---------------------------------------------------------------------

def mark_missing_orders_in_window(date_from: datetime, date_to: datetime, seen_order_ids: List[str]) -> None:
    """
    Отмечает ozon_missing для заказов в окне, которые не пришли из API.
    Требует колонок:
      orders.ozon_missing BOOLEAN
      orders.ozon_missing_at TIMESTAMP
    """
    # 1) увиденные — точно не missing
    if seen_order_ids:
        execute_query(
            """
            UPDATE orders
            SET ozon_missing = false,
                ozon_missing_at = NULL
            WHERE order_id = ANY(%s);
            """,
            (seen_order_ids,),
        )

    # 2) те, что в окне по order_date, но не пришли — missing
    execute_query(
        """
        UPDATE orders
        SET ozon_missing = true,
            ozon_missing_at = NOW()
        WHERE order_date >= %s
          AND order_date < %s
          AND (
            %s = '{}'::text[]
            OR NOT (order_id = ANY(%s))
          );
        """,
        (date_from, date_to, seen_order_ids, seen_order_ids),
    )


# ---------------------------------------------------------------------
# Main ETL entrypoint
# ---------------------------------------------------------------------

def load_fbo_orders_for_period(date_from: datetime, date_to: datetime) -> None:
    client = get_default_seller_client()

    print(f"[orders] load FBO postings: {date_from} .. {date_to}")
    postings = client.get_postings_fbo(date_from=date_from, date_to=date_to, limit=100)
    print(f"[orders] postings fetched: {len(postings)}")

    seen_order_ids: List[str] = []

    for p in postings:
        order_id = p.get("posting_number")
        if order_id:
            seen_order_ids.append(str(order_id))
        upsert_order_from_posting(p)

    # опционально: помечаем пропавшие в пределах окна
    # (если у тебя в миграциях добавлены ozon_missing/ozon_missing_at)
    try:
        mark_missing_orders_in_window(date_from, date_to, seen_order_ids)
    except Exception as e:
        # не валим ETL, если колонок ещё нет
        print(f"[orders] missing marking skipped: {e}")

    print("[orders] recalc customers / first order flags / finance ...")
    recalc_customers_aggregates()
    recalc_is_first_order_flags()
    recalc_orders_finance()
    recalc_orders_fees_breakdown()
    print("[orders] OK ✅")


def load_fbo_orders_last_n_days(days: int = 30) -> None:
    date_to = datetime.utcnow()
    date_from = date_to - timedelta(days=days)
    load_fbo_orders_for_period(date_from=date_from, date_to=date_to)


if __name__ == "__main__":
    # локальный ручной запуск
    load_fbo_orders_last_n_days(30)