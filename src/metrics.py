# src/metrics.py

"""
Модуль для расчёта метрик по данным OZON.

Задачи:
- Предоставить функции, которые возвращают готовые цифры для дашборда:
  - общее число заказов
  - число покупателей
  - общая выручка
  - средний чек
  - средняя выручка на клиента (LTV в рамках выбранного фильтра)
  - распределение клиентов по числу заказов (retention)
  - когорты по месяцу первого заказа

Фильтры:
- date_from / date_to   — по дате заказа (orders.order_date)
- campaign              — по рекламной кампании (orders.campaign)
- first_order_only      — только первые / только повторные / все заказы
- flavor                — вкус корма (products.flavor)
- grams                 — граммовка (products.grams)

Для фильтров flavor/grams мы делаем JOIN:
    orders -> order_items -> products
и аккуратно защищаемся от дублей заказов через DISTINCT / подзапросы.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from .db import fetch_one, fetch_all


# ---------- ВСПОМОГАТЕЛЬНЫЙ КОНСТРУКТОР ФИЛЬТРОВ ----------


def build_orders_filter(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    first_order_only: Optional[bool] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> Tuple[str, tuple, bool]:
    """
    Сформировать WHERE-условие и параметры для фильтрации по таблице orders.

    Параметры:
    - date_from: если задано — берём заказы с order_date >= date_from
    - date_to:   если задано — берём заказы с order_date <= date_to
    - campaign:  если задано — фильтруем по полю campaign (точное совпадение)
    - first_order_only:
        * True  → только первые заказы (is_first_order = TRUE)
        * False → только повторные (is_first_order = FALSE)
        * None  → не фильтровать
    - flavor: фильтр по products.flavor
    - grams:  фильтр по products.grams

    Возвращает:
    - where_sql: строка вида "WHERE ...", либо "" если фильтров нет
    - params:    кортеж параметров для подстановки в запрос
    - needs_product_join: True, если нужны JOIN'ы к order_items/products
    """
    conditions: List[str] = []
    params: List[Any] = []
    needs_product_join = False

    if date_from is not None:
        conditions.append("o.order_date >= %s")
        params.append(date_from)

    if date_to is not None:
        conditions.append("o.order_date <= %s")
        params.append(date_to)

    if campaign is not None:
        conditions.append("o.campaign = %s")
        params.append(campaign)

    if first_order_only is True:
        conditions.append("o.is_first_order = TRUE")
    elif first_order_only is False:
        conditions.append("o.is_first_order = FALSE")

    # --- фильтры по товарам ---
    if flavor is not None:
        conditions.append("p.flavor = %s")
        params.append(flavor)
        needs_product_join = True

    if grams is not None:
        conditions.append("p.grams = %s")
        params.append(grams)
        needs_product_join = True

    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)
    else:
        where_sql = ""

    return where_sql, tuple(params), needs_product_join


def build_join_sql(needs_product_join: bool) -> str:
    """
    Вернуть кусок SQL с JOIN'ами, если нужны фильтры по товарам.

    Если flavor/grams не используются — JOIN не нужен.
    """
    if not needs_product_join:
        return ""

    # INNER JOIN, т.к. если мы фильтруем по товару,
    # нас интересуют только заказы, в которых он есть.
    return """
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p ON p.sku = oi.sku
    """


# ---------- БАЗОВЫЕ МЕТРИКИ ----------


def get_total_orders(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    first_order_only: Optional[bool] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> int:
    """
    Общее число заказов с учётом фильтров.
    При наличии JOIN по товарам считаем DISTINCT по order_id.
    """
    where_sql, params, needs_join = build_orders_filter(
        date_from, date_to, campaign, first_order_only, flavor, grams
    )
    join_sql = build_join_sql(needs_join)

    query = f"""
    SELECT COUNT(DISTINCT o.order_id) AS cnt
    FROM orders o
    {join_sql}
    {where_sql};
    """

    row = fetch_one(query, params)
    return int(row["cnt"]) if row and row["cnt"] is not None else 0


def get_total_customers(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    first_order_only: Optional[bool] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> int:
    """
    Число уникальных клиентов (customer_id) среди заказов,
    подходящих под фильтры.
    """
    where_sql, params, needs_join = build_orders_filter(
        date_from, date_to, campaign, first_order_only, flavor, grams
    )
    join_sql = build_join_sql(needs_join)

    query = f"""
    SELECT COUNT(DISTINCT o.customer_id) AS cnt
    FROM orders o
    {join_sql}
    {where_sql};
    """

    row = fetch_one(query, params)
    return int(row["cnt"]) if row and row["cnt"] is not None else 0


def get_total_revenue(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    first_order_only: Optional[bool] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> float:
    """
    Общая выручка по заказам, подходящим под фильтры.

    При фильтре по товарам делаем подзапрос с DISTINCT order_id,
    чтобы не задвоить выручку при JOIN'ах.
    """
    where_sql, params, needs_join = build_orders_filter(
        date_from, date_to, campaign, first_order_only, flavor, grams
    )
    join_sql = build_join_sql(needs_join)

    if not needs_join:
        query = f"""
        SELECT SUM(o.revenue) AS total_revenue
        FROM orders o
        {where_sql};
        """
    else:
        # сначала выбираем уникальные заказы и их выручку, потом суммируем
        query = f"""
        WITH filtered_orders AS (
            SELECT DISTINCT o.order_id, o.revenue
            FROM orders o
            {join_sql}
            {where_sql}
        )
        SELECT SUM(revenue) AS total_revenue
        FROM filtered_orders;
        """

    row = fetch_one(query, params)
    value = row["total_revenue"] if row and row["total_revenue"] is not None else 0
    return float(value)


def get_avg_order_value(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    first_order_only: Optional[bool] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> float:
    """
    Средний чек: средняя сумма заказа среди выбранных заказов.

    При наличии JOIN'ов берём DISTINCT orders, затем считаем AVG по ним.
    """
    where_sql, params, needs_join = build_orders_filter(
        date_from, date_to, campaign, first_order_only, flavor, grams
    )
    join_sql = build_join_sql(needs_join)

    if not needs_join:
        query = f"""
        SELECT AVG(o.revenue) AS avg_order_value
        FROM orders o
        {where_sql};
        """
    else:
        query = f"""
        WITH filtered_orders AS (
            SELECT DISTINCT o.order_id, o.revenue
            FROM orders o
            {join_sql}
            {where_sql}
        )
        SELECT AVG(revenue) AS avg_order_value
        FROM filtered_orders;
        """

    row = fetch_one(query, params)
    value = row["avg_order_value"] if row and row["avg_order_value"] is not None else 0
    return float(value)


def get_avg_revenue_per_customer(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    first_order_only: Optional[bool] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> float:
    """
    Средняя выручка на клиента (LTV в рамках выбранного фильтра).

    Логика:
    - для каждого клиента считаем суммарную выручку по его заказам (с учётом фильтров),
    - берём среднее по этим клиентам.
    """
    where_sql, params, needs_join = build_orders_filter(
        date_from, date_to, campaign, first_order_only, flavor, grams
    )
    join_sql = build_join_sql(needs_join)

    if not needs_join:
        query = f"""
        WITH customer_totals AS (
            SELECT
                o.customer_id,
                SUM(o.revenue) AS total_revenue
            FROM orders o
            {where_sql}
            GROUP BY o.customer_id
        )
        SELECT AVG(total_revenue) AS avg_revenue_per_customer
        FROM customer_totals;
        """
    else:
        # При JOIN'ах сначала выделяем уникальные заказы,
        # затем агрегируем по клиентам.
        query = f"""
        WITH filtered_orders AS (
            SELECT DISTINCT o.order_id, o.customer_id, o.revenue
            FROM orders o
            {join_sql}
            {where_sql}
        ),
        customer_totals AS (
            SELECT
                customer_id,
                SUM(revenue) AS total_revenue
            FROM filtered_orders
            GROUP BY customer_id
        )
        SELECT AVG(total_revenue) AS avg_revenue_per_customer
        FROM customer_totals;
        """

    row = fetch_one(query, params)
    value = (
        row["avg_revenue_per_customer"]
        if row and row["avg_revenue_per_customer"] is not None
        else 0
    )
    return float(value)


# ---------- RETENTION: РАСПРЕДЕЛЕНИЕ ПО КОЛИЧЕСТВУ ЗАКАЗОВ ----------


def get_retention_distribution(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Распределение клиентов по числу заказов в рамках выбранного фильтра.

    Логика:
    - берём заказы с нужными фильтрами (включая вкус/граммовку),
    - считаем количество заказов на клиента,
    - группируем клиентов по этому числу.
    """
    where_sql, params, needs_join = build_orders_filter(
        date_from, date_to, campaign, first_order_only=None, flavor=flavor, grams=grams
    )
    join_sql = build_join_sql(needs_join)

    query = f"""
    WITH filtered_orders AS (
        SELECT DISTINCT o.order_id, o.customer_id
        FROM orders o
        {join_sql}
        {where_sql}
    ),
    customer_orders AS (
        SELECT
            customer_id,
            COUNT(*) AS orders_count
        FROM filtered_orders
        GROUP BY customer_id
    )
    SELECT
        orders_count,
        COUNT(*) AS customers_count
    FROM customer_orders
    GROUP BY orders_count
    ORDER BY orders_count;
    """

    rows = fetch_all(query, params)
    return [
        {
            "orders_count": int(r["orders_count"]),
            "customers_count": int(r["customers_count"]),
        }
        for r in rows
    ]


# ---------- КОГОРТЫ ПО МЕСЯЦУ ПЕРВОГО ЗАКАЗА ----------


def get_cohort_by_first_order_month() -> List[Dict[str, Any]]:
    """
    Когортная таблица по месяцу первого заказа.

    Источник — таблица customers (там already есть first_order_date, total_revenue).

    Без фильтров по продуктам — это "общая" когортная картинка.
    """
    query = """
    SELECT
        DATE_TRUNC('month', first_order_date)::date AS cohort_month,
        COUNT(*) AS customers_count,
        SUM(total_revenue) AS cohort_revenue,
        AVG(total_revenue) AS avg_revenue_per_customer
    FROM customers
    WHERE first_order_date IS NOT NULL
    GROUP BY cohort_month
    ORDER BY cohort_month;
    """

    rows = fetch_all(query)

    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "cohort_month": r["cohort_month"],
                "customers_count": int(r["customers_count"]),
                "cohort_revenue": float(r["cohort_revenue"] or 0),
                "avg_revenue_per_customer": float(
                    r["avg_revenue_per_customer"] or 0
                ),
            }
        )
    return result


# ---------- ОБЩИЙ САММАРИ-БЛОК ДЛЯ ДАШБОРДА ----------


def get_summary(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    campaign: Optional[str] = None,
    first_order_only: Optional[bool] = None,
    flavor: Optional[str] = None,
    grams: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Удобная функция: вернуть сразу набор ключевых метрик.
    Это удобно использовать в дашборде как "общий блок".
    """
    return {
        "total_orders": get_total_orders(
            date_from, date_to, campaign, first_order_only, flavor, grams
        ),
        "total_customers": get_total_customers(
            date_from, date_to, campaign, first_order_only, flavor, grams
        ),
        "total_revenue": get_total_revenue(
            date_from, date_to, campaign, first_order_only, flavor, grams
        ),
        "avg_order_value": get_avg_order_value(
            date_from, date_to, campaign, first_order_only, flavor, grams
        ),
        "avg_revenue_per_customer": get_avg_revenue_per_customer(
            date_from, date_to, campaign, first_order_only, flavor, grams
        ),
    }
