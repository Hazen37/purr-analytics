# src/finance_report_loader.py

import re
import pandas as pd
from decimal import Decimal
from typing import Optional

from .db import execute_query
from .etl import recalc_orders_finance  # ты уже добавил эту функцию

POSTING_RE = re.compile(r"^\d+-\d+-\d+$")


def _to_decimal(x) -> Decimal:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return Decimal("0")
    return Decimal(str(x).replace(" ", "").replace(",", "."))


def _normalize_order_id(x) -> Optional[str]:
    """
    В отчёте:
    - для строк заказа ID начисления часто = posting_number (36387264-0119-1)
    - для расходов рекламы может быть число или пусто → тогда order_id = None
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    return s if POSTING_RE.match(s) else None


def load_accruals_report_xlsx(path: str):
    # В отчёте первая строка = "Период: ....", заголовки начинаются со 2-й строки
    df = pd.read_excel(path, header=1)

    required = [
        "ID начисления",
        "Дата начисления",
        "Группа услуг",
        "Тип начисления",
        "SKU",
        "Дата принятия заказа в обработку или оказания услуги",
        "Сумма итого, руб.",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"В отчёте не найдены колонки: {missing}")

    # Важно: чтобы ETL был идемпотентный — удалим строки этого источника за тот же период файла.
    # (Можно позже улучшить до удаления только по конкретным order_id.)
    execute_query("DELETE FROM order_fee_items WHERE source = 'finance_report';")

    insert_q = """
    INSERT INTO order_fee_items (
      order_id, fee_group, fee_name, amount,
      percent, product_id, source, sku, occurred_at, operation_type
    )
    VALUES (%s,%s,%s,%s,%s,%s,'finance_report',%s,%s,%s);
    """

    for _, row in df.iterrows():
        order_id = _normalize_order_id(row["ID начисления"])
        fee_group = str(row["Группа услуг"]).strip() if pd.notna(row["Группа услуг"]) else None
        fee_type = str(row["Тип начисления"]).strip() if pd.notna(row["Тип начисления"]) else None
        amount = _to_decimal(row["Сумма итого, руб."])

        sku = None
        if pd.notna(row["SKU"]):
            try:
                sku = int(row["SKU"])
            except Exception:
                sku = None

        occurred_at = row["Дата принятия заказа в обработку или оказания услуги"]
        accrual_date = row["Дата начисления"]

        # percent в этом отчёте бывает в "Вознаграждение Ozon, %"
        percent = None
        if "Вознаграждение Ozon, %" in df.columns and pd.notna(row.get("Вознаграждение Ozon, %")):
            percent = _to_decimal(row["Вознаграждение Ozon, %"])

        # product_id у нас сейчас используется из posting.financial_data.
        # В отчёте его нет — оставим NULL (позже можно добавить отдельную колонку sku и анализировать по SKU).
        product_id = None

        execute_query(insert_q, (
            order_id,
            fee_group,
            fee_type,      # fee_name = тип начисления (коротко и удобно)
            amount,
            percent,
            product_id,
            sku,
            occurred_at if pd.notna(occurred_at) else None,
            fee_type,      # operation_type = тот же тип начисления
        ))

    # После загрузки отчёта пересчитываем totals в orders
    recalc_orders_finance()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m src.finance_report_loader <path_to_xlsx>")

    load_accruals_report_xlsx(sys.argv[1])
    print("[finance_report_loader] OK")
