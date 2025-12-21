# src/update_all.py
"""
Обновление всей базы одной командой.

Запуск:
  python -m src.update_all 2025-10-01 2025-12-12
"""

from datetime import datetime
from .etl import (
    load_fbo_orders_for_period,
    recalc_orders_finance,
    recalc_orders_fees_breakdown,
)
from .finance_report_etl import run as run_finance_etl
from .period_costs_etl import recalc_period_costs

def update_all(date_from: str, date_to: str):
    # 1) Заказы из Seller API
    load_fbo_orders_for_period(
        date_from=datetime.strptime(date_from, "%Y-%m-%d"),
        date_to=datetime.strptime(date_to, "%Y-%m-%d"),
    )

    # 2) Финансы/транзакции из finance API (логистика, эквайринг и т.д.)
    run_finance_etl(date_from, date_to)

    # 3) Пересчёты по заказам (на всякий случай после finance)
    recalc_orders_finance()
    recalc_orders_fees_breakdown()

    # 4) Агрегат периодных расходов (CPC, баллы, FBO-обработка и т.п.)
    recalc_period_costs(date_from, date_to)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m src.update_all YYYY-MM-DD YYYY-MM-DD")
    update_all(sys.argv[1], sys.argv[2])
    print("[update_all] OK ✅")
