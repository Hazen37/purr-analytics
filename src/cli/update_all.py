# src/cli/update_all.py
"""
Обновление всей базы одной командой.

Запуск:
  python -m src.cli.update_all
  python -m src.cli.update_all 2025-10-01 2025-12-12

Идея:
- всегда прогоняем миграции (идемпотентно)
- считаем диапазон дат (по умолчанию LOOKBACK)
- выполняем шаги ETL по очереди
- добавляем понятные логи + тайминги
- performance можно отключать флагами env, чтобы не валить весь ETL
"""

from __future__ import annotations

import os
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional, Tuple

from src.migrations.run import run as run_migrations

from src.etl.orders.load_orders import load_fbo_orders_for_period
from src.etl.finance.finance_api import run as run_finance_api
from src.etl.finance.period_costs import recalc_period_costs as run_period_costs

from src.etl.performance.campaigns import load_campaigns as run_perf_campaigns
from src.etl.performance.daily import run as run_perf_daily
from src.etl.performance.orders import run as run_perf_orders

from src.etl.reviews.load_reviews import load_reviews

from src.etl.analytics.sku_day_metrics import load_sku_day_metrics

from src.etl.stocks.load_stocks import load_stocks_current

from src.core.db import execute_query

LOOKBACK_DAYS = int(os.getenv("ETL_LOOKBACK_DAYS", "30"))

# Флаги включения этапов (по умолчанию: только orders+finance+period_costs)
ENABLE_PERFORMANCE = os.getenv("ETL_ENABLE_PERFORMANCE", "0") == "1"
ENABLE_PERF_CAMPAIGNS = os.getenv("ETL_ENABLE_PERF_CAMPAIGNS", "1") == "1"
ENABLE_PERF_DAILY = os.getenv("ETL_ENABLE_PERF_DAILY", "1") == "1"
ENABLE_PERF_ORDERS = os.getenv("ETL_ENABLE_PERF_ORDERS", "1") == "1"
ENABLE_REVIEWS = os.getenv("ETL_ENABLE_REVIEWS", "1") == "1"
ENABLE_ANALYTICS = os.getenv("ETL_ENABLE_ANALYTICS", "1") == "1"
ENABLE_STOCKS = os.getenv("ETL_ENABLE_STOCKS", "1") == "1"

# Если 1 — падать на любом этапе; если 0 — продолжать (кроме orders/migrations)
STRICT_MODE = os.getenv("ETL_STRICT_MODE", "1") == "1"

# Удобно, когда запускаем в контейнере и хотим видеть время в логах
LOG_TIME_UTC = os.getenv("ETL_LOG_TIME_UTC", "1") == "1"


def _now_str() -> str:
    if LOG_TIME_UTC:
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def log(msg: str) -> None:
    print(f"[update_all] {_now_str()} {msg}", flush=True)

from textwrap import dedent

TEST_CUSTOMER_ID = os.getenv("TEST_CUSTOMER_ID", "47533921")

def recreate_orders_clean_view() -> None:
    """
    НЕЛЬЗЯ использовать CREATE OR REPLACE VIEW, если меняется список колонок (o.*).
    Поэтому делаем DROP + CREATE, как в recalc.sh.
    """
    cid = TEST_CUSTOMER_ID.replace("'", "''")

    # 1) drop
    execute_query("DROP VIEW IF EXISTS public.orders_clean;")

    # 2) create
    sql = f"""
    CREATE VIEW public.orders_clean AS
    SELECT
      o.*,
      COALESCE(o.delivery_city, '(нет города)') || ' / ' ||
      COALESCE(o.warehouse_name, '(нет склада)') AS delivery_cluster
    FROM public.orders o
    WHERE o.customer_id <> '{cid}';
    """.strip()

    execute_query(sql)

def _compute_range(date_from: str | None, date_to: str | None) -> tuple[str, str]:
    """
    Возвращает (YYYY-MM-DD, YYYY-MM-DD).
    date_to — включительно по дню (как у тебя было раньше).
    """
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") if date_to else datetime.utcnow()
    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else (to_dt - timedelta(days=LOOKBACK_DAYS))
    return from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d")


@dataclass
class Step:
    name: str
    fn: Callable[[], None]
    required: bool = False  # если True — ошибка валит весь ETL


def _run_step(step: Step) -> None:
    log(f"▶️  step start: {step.name}")
    t0 = time.time()
    try:
        step.fn()
        dt = time.time() - t0
        log(f"✅ step ok: {step.name} ({dt:.1f}s)")
    except Exception as e:
        dt = time.time() - t0
        log(f"❌ step failed: {step.name} ({dt:.1f}s) err={type(e).__name__}: {e}")
        # печатаем traceback целиком (в docker logs удобно)
        tb = traceback.format_exc()
        print(tb, flush=True)

        if step.required or STRICT_MODE:
            raise
        else:
            log(f"⚠️  continue after failure (STRICT_MODE=0): {step.name}")


def update_all(date_from: str | None = None, date_to: str | None = None) -> None:
    date_from_s, date_to_s = _compute_range(date_from, date_to)

    log("========================================")
    log("ETL start")
    log(f"range: {date_from_s} .. {date_to_s} (LOOKBACK_DAYS={LOOKBACK_DAYS})")
    log(f"flags: PERFORMANCE={int(ENABLE_PERFORMANCE)} "
        f"PERF_CAMPAIGNS={int(ENABLE_PERF_CAMPAIGNS)} "
        f"PERF_DAILY={int(ENABLE_PERF_DAILY)} "
        f"PERF_ORDERS={int(ENABLE_PERF_ORDERS)} "
        f"STRICT_MODE={int(STRICT_MODE)}")
    log("========================================")

    # Заказы — удобнее передать datetime (как у тебя)
    date_from_dt = datetime.strptime(date_from_s, "%Y-%m-%d")
    date_to_dt   = datetime.strptime(date_to_s, "%Y-%m-%d") + timedelta(days=1)

    steps: list[Step] = [
        Step(
            name="migrations",
            fn=lambda: run_migrations(),
            required=True,
        ),
        Step(
            name="views: recreate orders_clean",
            fn=lambda: recreate_orders_clean_view(),
            required=True,
        ),
        Step(
            name="orders (seller api)",
            fn=lambda: load_fbo_orders_for_period(date_from=date_from_dt, date_to=date_to_dt),
            required=True,
        ),
        Step(
            name="orders: recalc order numbers",
            fn=lambda: execute_query("SELECT public.recalc_order_numbers();"),
            required=True,
        ),
        # Step(
        #     name="reviews (seller api)",
        #     fn=lambda: load_reviews(limit=100),
        #     required=False,
        # ),
        Step(
            name="finance (seller finance api)",
            fn=lambda: run_finance_api(date_from_s, date_to_s),
            required=False,
        ),
        Step(
            name="period_costs (recalc aggregates)",
            fn=lambda: run_period_costs(date_from_s, date_to_s),
            required=False,
        ),
    ]

    if ENABLE_PERFORMANCE:
        if ENABLE_PERF_CAMPAIGNS:
            steps.append(
                Step(
                    name="performance campaigns (catalog)",
                    fn=lambda: run_perf_campaigns(),
                    required=False,
                )
            )
        else:
            log("skip: performance campaigns (ETL_ENABLE_PERF_CAMPAIGNS=0)")

        if ENABLE_PERF_DAILY:
            steps.append(
                Step(
                    name="performance daily",
                    fn=lambda: run_perf_daily(date_from_s, date_to_s),
                    required=False,
                )
            )
        else:
            log("skip: performance daily (ETL_ENABLE_PERF_DAILY=0)")

        if ENABLE_PERF_ORDERS:
            steps.append(
                Step(
                    name="performance orders attribution",
                    fn=lambda: run_perf_orders(date_from_s, date_to_s),
                    required=False,
                )
            )
        else:
            log("skip: performance orders (ETL_ENABLE_PERF_ORDERS=0)")
    else:
        log("skip: performance (ETL_ENABLE_PERFORMANCE=0)")
    if ENABLE_REVIEWS:
        steps.append(
            Step(
                name="reviews (seller api)",
                fn=lambda: load_reviews(limit=100),
                required=False,
            )
        )
    else:
        log("skip: reviews (ETL_ENABLE_REVIEWS=0)")
    if ENABLE_ANALYTICS:
        steps.append(
            Step(
                name="analytics (seller api): sku_day metrics",
                fn=lambda: load_sku_day_metrics(date_from_s, date_to_s, recalc_days=14),
                required=False,
            )
        )
    else:
        log("skip: analytics (ETL_ENABLE_ANALYTICS=0)")
    if ENABLE_STOCKS:
        steps.append(
            Step(
                name="stocks (seller api): current",
                fn=lambda: load_stocks_current(),
                required=False,
            )
        )
    else:
        log("skip: stocks (ETL_ENABLE_STOCKS=0)")

    t0 = time.time()
    for s in steps:
        _run_step(s)

    total = time.time() - t0
    log("========================================")
    log(f"ETL finished OK ({total:.1f}s)")
    log("========================================")


def _usage() -> str:
    return "Usage: python -m src.cli.update_all [YYYY-MM-DD YYYY-MM-DD]"


if __name__ == "__main__":
    if len(sys.argv) == 1:
        update_all()
    elif len(sys.argv) == 3:
        update_all(sys.argv[1], sys.argv[2])
    else:
        raise SystemExit(_usage())
