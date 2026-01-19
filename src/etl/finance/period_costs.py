# src/period_costs_etl.py
"""
Агрегируем периодные расходы (которые не привязаны к заказам):
order_fee_items.source='finance_api' AND order_id IS NULL

Запуск:
  python -m src.period_costs_etl 2025-10-01 2025-12-12
"""

from src.core.db import execute_query

def recalc_period_costs(date_from: str, date_to: str):
    # 1) удаляем старые агрегаты за период (чтобы ETL был идемпотентный)
    execute_query(
        """
        DELETE FROM finance_period_costs
        WHERE (cost_date >= %s AND cost_date < %s)
          OR cost_date IS NULL;
        """,
        (date_from, date_to),
    )

    # 2) вставляем агрегаты заново
    q = """
      INSERT INTO finance_period_costs (cost_date, fee_group, fee_name, amount)
      SELECT
        DATE(occurred_at) AS cost_date,
        fee_group,
        fee_name,
        SUM(amount)       AS amount
      FROM order_fee_items
      WHERE source = 'finance_api'
        AND order_id IS NULL
        AND occurred_at IS NOT NULL
        AND occurred_at >= %s
        AND occurred_at <  %s
      GROUP BY 1,2,3;
    """
    execute_query(q, (date_from, date_to))

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m src.period_costs_etl YYYY-MM-DD YYYY-MM-DD")
    recalc_period_costs(sys.argv[1], sys.argv[2])
    print("[period_costs] OK ✅")
