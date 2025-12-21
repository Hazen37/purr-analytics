# src/finance_report_etl.py
"""
ETL "прочих трат" из Ozon Seller API:
- тянем транзакции /v3/finance/transaction/list
- пишем строки удержаний/начислений в order_fee_items (source='finance_api')
- пересчитываем итог по заказам в orders (recalc_orders_finance)

Запуск:
  python -m src.finance_report_etl 2025-12-01 2025-12-12
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

import requests

from .config import settings
from .db import execute_query, fetch_one

def order_exists(order_id: str) -> bool:
    return fetch_one("SELECT 1 FROM orders WHERE order_id=%s LIMIT 1;", (order_id,)) is not None

def resolve_order_id(order_id_candidate: str | None) -> tuple[str | None, str | None]:
    """
    Возвращает (order_id_to_save, ext_order_id)
    - если нашли точное совпадение — пишем в order_id
    - если не нашли, но нашли 1 заказ по шаблону <candidate>-* — привязываем
    - иначе оставляем order_id NULL и пишем в ext_order_id
    """
    if not order_id_candidate:
        return None, None

    cand = str(order_id_candidate).strip()

    # 1) точное совпадение
    if order_exists(cand):
        return cand, None

    # 2) попытка найти order_id вида "<cand>-X"
    row = fetch_one(
        "SELECT order_id FROM orders WHERE order_id LIKE %s ORDER BY order_date DESC LIMIT 2;",
        (cand + "-%",)
    )
    if row and row.get("order_id"):
        # если матчей несколько — мы взяли самый свежий. Можно улучшить позже.
        return row["order_id"], cand

    return None, cand

from .etl import recalc_orders_finance

BASE_URL = "https://api-seller.ozon.ru"


def _dec(x: Any) -> Decimal:
    if x is None:
        return Decimal("0")
    return Decimal(str(x).replace(" ", "").replace(",", "."))

import time
import random

def _post(path: str, payload: dict) -> dict:
    url = f"{BASE_URL}{path}"
    headers = {
        "Client-Id": settings.OZON_CLIENT_ID,
        "Api-Key": settings.OZON_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # ретраи только для 500/502/503/504
    max_retries = 6
    base_sleep = 1.5

    for attempt in range(max_retries):
        r = requests.post(url, json=payload, headers=headers, timeout=90)

        if r.ok:
            return r.json()

        # если это временная ошибка сервера — пробуем ещё раз
        if r.status_code in (500, 502, 503, 504):
            sleep = base_sleep * (2 ** attempt) + random.uniform(0, 0.5)
            print(f"[finance] Ozon {r.status_code} on {path}, retry in {sleep:.1f}s...")
            time.sleep(sleep)
            continue

        # всё остальное — фатально
        raise RuntimeError(f"Ozon API error {r.status_code}: {r.text}")

    # если исчерпали ретраи
    raise RuntimeError(f"Ozon API error: too many retries, last={r.status_code}: {r.text}")

def _iso(dt: datetime) -> str:
    # Ozon обычно принимает ISO8601 с Z
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _guess_fee_group(service_name: str) -> str:
    s = (service_name or "").lower()

    # Комиссия
    if "вознаграж" in s or "комисс" in s or "commission" in s:
        return "Вознаграждение Ozon"

    # Логистика / продвижение
    if "logistic" in s or "логист" in s or "достав" in s or "last mile" in s or "courier" in s:
        return "Услуги доставки"

    # Эквайринг
    if "эквайр" in s or "acquiring" in s:
        return "Услуги агентов"

    # Реклама / продвижение
    if "клик" in s or "cpc" in s or "cpo" in s or "реклам" in s or "продвиж" in s:
        return "Продвижение и реклама"
    
    return "Прочее"

def normalize_fee_name(name: str) -> str:
    if not name:
        return "UNKNOWN"

    mapping = {
        "MarketplaceServiceItemDirectFlowLogistic": "Логистика (доставка)",
        "MarketplaceServiceItemRedistributionLastMileCourier": "Последняя миля (курьер)",
        "MarketplaceRedistributionOfAcquiringOperation": "Эквайринг",
    }
    return mapping.get(name, name)

def fetch_transactions(date_from: datetime, date_to: datetime, page: int, page_size: int = 200) -> dict:
    """
    /v3/finance/transaction/list — список транзакций.
    В одном запросе период ограничен (обычно 30 дней), поэтому мы режем по окнам.
    """
    payload = {
        "filter": {
            "date": {
                "from": _iso(date_from),
                "to": _iso(date_to),
            },
            # можно добавить operation_type, если захочешь фильтровать
        },
        "page": page,
        "page_size": page_size,
    }
    return _post("/v3/finance/transaction/list", payload)


def load_transactions_window(date_from: datetime, date_to: datetime):
    """
    Загружает транзакции за окно [date_from; date_to] и кладёт в order_fee_items.
    """
    insert_q = """
      INSERT INTO order_fee_items (
        order_id, ext_order_id, fee_group, fee_name, amount,
        operation_type, occurred_at, sku, source
      )
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'finance_api');
    """

    page = 1
    total_rows = 0

    while True:
        data = fetch_transactions(date_from, date_to, page=page, page_size=200)
        result = data.get("result") or {}
        operations = result.get("operations") or []
        # import json

        # if operations:
        #   print("=== SAMPLE OPERATION JSON ===")
        #   print(json.dumps(operations[0], ensure_ascii=False, indent=2))
        #   print("=== END SAMPLE OPERATION JSON ===")
        #   return 0
        
        if not operations:
            break

        for op in operations:
          # ✅ правильная привязка к заказу
          posting = op.get("posting") or {}
          posting_number = (
              posting.get("posting_number")
              or op.get("posting_number")
              or op.get("order_id")
              or None
          )
          order_id = str(posting_number).strip() if posting_number else None

          # order_id_candidate = order_id

          # if order_id_candidate and order_exists(order_id_candidate):
          #     order_id_to_save = order_id_candidate
          #     ext_order_id = None
          # else:
          #     order_id_to_save = None
          #     ext_order_id = order_id_candidate

          order_id_to_save, ext_order_id = resolve_order_id(order_id)

          op_date = op.get("operation_date") or op.get("date") or None
          op_type = op.get("operation_type") or None
          op_type_name = op.get("operation_type_name") or op_type or "UNKNOWN"
          op_amount = _dec(op.get("amount"))

          services = op.get("services") or []

          # ✅ Если services пустой — пишем одну строку по операции
          if not services:
              fee_group = _guess_fee_group(str(op_type_name))
              execute_query(insert_q, (
                  order_id_to_save,          # order_id
                  ext_order_id,              # ext_order_id
                  fee_group,                 # fee_group
                  normalize_fee_name(str(op_type_name)),  # fee_name
                  op_amount,                 # amount
                  str(op_type) if op_type else None,  # operation_type
                  op_date,                   # occurred_at
                  None,                      # sku
              ))
              total_rows += 1
              continue


          # ✅ Иначе — пишем построчно по services
          for svc in services:
            name = svc.get("name") or svc.get("type") or "UNKNOWN"
            amount = svc.get("price")
            if amount is None:
                amount = svc.get("amount")
            amount_dec = _dec(amount)

            sku = svc.get("sku") or None
            if sku is not None:
                try:
                    sku = int(sku)
                except Exception:
                    sku = None

            fee_group = _guess_fee_group(str(name))

            execute_query(insert_q, (
                order_id_to_save,          # order_id
                ext_order_id,              # ext_order_id
                fee_group,                 # fee_group
                normalize_fee_name(str(name)),  # fee_name
                amount_dec,                # amount
                str(op_type) if op_type else None,  # operation_type
                op_date,                   # occurred_at
                sku,                       # sku
            ))
            total_rows += 1


        # пагинация
        # Часто в ответе есть page_count/total — но чтобы не гадать, делаем простой критерий:
        if len(operations) < 200:
            break
        page += 1


    return total_rows


def run(date_from_str: str, date_to_str: str):
    # парсим даты
    date_from = datetime.strptime(date_from_str, "%Y-%m-%d")
    date_to = datetime.strptime(date_to_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)

    # чистим предыдущую загрузку API
    execute_query("DELETE FROM order_fee_items WHERE source='finance_api';")

    # режем период на окна по 10 дней (ограничение метода). :contentReference[oaicite:4]{index=4}
    window = timedelta(days=10)
    cur_from = date_from
    total = 0

    while cur_from <= date_to:
        cur_to = min(cur_from + window - timedelta(seconds=1), date_to)
        print(f"[finance] Окно: {cur_from.date()} — {cur_to.date()} ...")
        total += load_transactions_window(cur_from, cur_to)
        cur_from = cur_to + timedelta(seconds=1)

    print(f"[finance] Строк в order_fee_items (finance_api): {total}")

    print("[finance] Пересчитываем orders...")
    recalc_orders_finance()
    print("[finance] OK ✅")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m src.finance_report_etl YYYY-MM-DD YYYY-MM-DD")
    run(sys.argv[1], sys.argv[2])
