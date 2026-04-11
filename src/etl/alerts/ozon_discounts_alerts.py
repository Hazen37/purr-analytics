import json
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv

from src.core.db import get_connection
from src.ozon.seller_api import get_default_seller_client

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_message(text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        },
        timeout=15,
    )
    if not resp.ok:
        raise RuntimeError(f"Telegram error: {resp.text}")


def fetch_current_action_products() -> List[Dict[str, Any]]:
    client = get_default_seller_client()
    actions_resp = client.get_actions()
    actions = actions_resp.get("result", []) or []

    raw_rows: List[Dict[str, Any]] = []

    for action in actions:
        if not action.get("is_participating"):
            continue

        action_id = action["id"]
        action_title = action.get("title") or f"Action {action_id}"

        offset = 0
        limit = 1000

        while True:
            products_resp = client.get_action_products(
                action_id=action_id,
                limit=limit,
                offset=offset,
            )
            result = products_resp.get("result", {}) or {}
            products = result.get("products", []) or []

            for p in products:
                raw_rows.append(
                    {
                        "action_id": int(action_id),
                        "action_title": action_title,
                        "product_id": int(p["id"]),
                        "price": p.get("price"),
                        "action_price": p.get("action_price"),
                        "current_boost": p.get("current_boost"),
                        "stock": p.get("stock"),
                        "add_mode": p.get("add_mode"),
                    }
                )

            if not products or len(products) < limit:
                break

            offset += limit

    enrich_missing_product_mapping([x["product_id"] for x in raw_rows])

    product_id_map = load_product_id_map()
    products_map = load_products_map()

    rows: List[Dict[str, Any]] = []
    for row in raw_rows:
        mapping = product_id_map.get(row["product_id"], {})
        sku = mapping.get("sku")
        product_info = products_map.get(sku, {}) if sku is not None else {}

        product_name = (
            mapping.get("ozon_name")
            or product_info.get("name")
        )

        rows.append(
            {
                **row,
                "sku": sku,
                "product_name": product_name,
                "offer_id": mapping.get("offer_id"),
                "min_price": mapping.get("min_price"),
            }
        )

    return rows


def load_previous_state() -> Dict[str, Dict[str, Any]]:
    sql = """
        SELECT key, payload_json
        FROM public.ozon_alert_state
        WHERE is_active = true
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    result: Dict[str, Dict[str, Any]] = {}
    for key, payload_json in rows:
        result[key] = payload_json
    return result


def make_key(item: Dict[str, Any]) -> str:
    return f"action:{item['action_id']}:product:{item['product_id']}"


def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "action_id": item["action_id"],
        "action_title": item["action_title"],
        "product_id": item["product_id"],
        "sku": item.get("sku"),
        "product_name": item.get("product_name"),
        "offer_id": item.get("offer_id"),
        "min_price": float(item["min_price"]) if item.get("min_price") is not None else None,
        "price": float(item["price"]) if item.get("price") is not None else None,
        "action_price": float(item["action_price"]) if item.get("action_price") is not None else None,
        "current_boost": float(item["current_boost"]) if item.get("current_boost") is not None else None,
        "stock": int(item["stock"]) if item.get("stock") is not None else None,
        "add_mode": item.get("add_mode"),
    }


def diff_states(
    current_items: List[Dict[str, Any]],
    previous_state: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Tuple[Dict[str, Any], Dict[str, Any]]], List[Dict[str, Any]]]:
    current_map = {make_key(x): normalize_item(x) for x in current_items}

    new_items: List[Dict[str, Any]] = []
    changed_items: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    ended_items: List[Dict[str, Any]] = []

    for key, cur in current_map.items():
        prev = previous_state.get(key)
        if prev is None:
            new_items.append(cur)
        elif prev != cur:
            changed_items.append((prev, cur))

    for key, prev in previous_state.items():
        if key not in current_map:
            ended_items.append(prev)

    return new_items, changed_items, ended_items


def save_snapshot(items: List[Dict[str, Any]]) -> None:
    sql = """
        INSERT INTO public.ozon_action_products_snapshot (
            action_id,
            action_title,
            product_id,
            price,
            action_price,
            current_boost,
            stock,
            add_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for x in items:
                cur.execute(
                    sql,
                    (
                        x["action_id"],
                        x["action_title"],
                        x["product_id"],
                        x.get("price"),
                        x.get("action_price"),
                        x.get("current_boost"),
                        x.get("stock"),
                        x.get("add_mode"),
                    ),
                )
        conn.commit()


def upsert_state(items: List[Dict[str, Any]]) -> None:
    current_map = {make_key(x): normalize_item(x) for x in items}
    current_keys = set(current_map.keys())

    with get_connection() as conn:
        with conn.cursor() as cur:
            for key, payload in current_map.items():
                cur.execute(
                    """
                    INSERT INTO public.ozon_alert_state (
                        key, alert_type, payload_json, first_seen_at, last_seen_at, is_active
                    )
                    VALUES (%s, %s, %s::jsonb, now(), now(), true)
                    ON CONFLICT (key)
                    DO UPDATE SET
                        payload_json = EXCLUDED.payload_json,
                        last_seen_at = now(),
                        is_active = true
                    """,
                    (key, "action_product", json.dumps(payload, ensure_ascii=False)),
                )

            cur.execute("SELECT key FROM public.ozon_alert_state WHERE is_active = true")
            existing_keys = {row[0] for row in cur.fetchall()}

            to_disable = existing_keys - current_keys
            for key in to_disable:
                cur.execute(
                    """
                    UPDATE public.ozon_alert_state
                    SET is_active = false, last_seen_at = now()
                    WHERE key = %s
                    """,
                    (key,),
                )

        conn.commit()


def format_new_alert(item: Dict[str, Any]) -> str:
    title = item.get("product_name") or f"Product ID {item['product_id']}"
    sku_line = f"SKU: {item.get('sku')}\n" if item.get("sku") is not None else ""

    return (
        f"🔥 <b>Товар добавлен в акцию Ozon</b>\n\n"
        f"{title}\n"
        f"{sku_line}"
        f"Product ID: {item['product_id']}\n"
        f"Акция: {item['action_title']}\n\n"
        f"Цена: {item.get('price')}\n"
        f"Акционная цена: {item.get('action_price')}\n"
        f"Буст: {item.get('current_boost')}\n"
        f"Остаток: {item.get('stock')}\n"
        f"Режим: {item.get('add_mode')}"
    )


def format_changed_alert(prev: Dict[str, Any], cur: Dict[str, Any]) -> str:
    changes = []

    for field, label in [
        ("price", "Цена"),
        ("action_price", "Акционная цена"),
        ("current_boost", "Буст"),
        ("stock", "Остаток"),
        ("add_mode", "Режим"),
    ]:
        if prev.get(field) != cur.get(field):
            changes.append(f"{label}: {prev.get(field)} → {cur.get(field)}")

    if not changes:
        return None  # ❗ ключевая строка

    return (
        f"⚠️ <b>Изменилась акция Ozon</b>\n\n"
        f"{cur.get('product_name')}\n"
        f"SKU: {cur.get('sku')}\n"
        f"Акция: {cur['action_title']}\n\n"
        + "\n".join(changes)
    )


def format_ended_alert(item: Dict[str, Any]) -> str:
    title = item.get("product_name") or f"Product ID {item['product_id']}"
    sku_line = f"SKU: {item.get('sku')}\n" if item.get("sku") is not None else ""

    return (
        f"❌ <b>Товар убрали из акции Ozon</b>\n\n"
        f"{title}\n"
        f"{sku_line}"
        f"Product ID: {item['product_id']}\n"
        f"Акция: {item['action_title']}\n\n"
        f"Последняя цена: {item.get('price')}\n"
        f"Последняя акционная цена: {item.get('action_price')}\n"
        f"Последний буст: {item.get('current_boost')}"
    )

def chunk_text(text: str, max_len: int = 3500) -> List[str]:
    chunks = []
    while text:
        chunks.append(text[:max_len])
        text = text[max_len:]
    return chunks

def run_ozon_alerts_check() -> None:
    print("▶️ checking ozon actions alerts...")

    current_items = fetch_current_action_products()
    previous_state = load_previous_state()

    new_items, changed_items, ended_items = diff_states(current_items, previous_state)

    below_min_items = collect_below_min_price_items(current_items)
    previous_below_min_state = load_below_min_state()
    new_below_min_items, changed_below_min_items, ended_below_min_items = diff_below_min_states(
        below_min_items,
        previous_below_min_state,
    )

    print(
        f"new={len(new_items)}, changed={len(changed_items)}, ended={len(ended_items)}, "
        f"below_min_new={len(new_below_min_items)}, "
        f"below_min_changed={len(changed_below_min_items)}, "
        f"below_min_ended={len(ended_below_min_items)}"
    )

    messages = []

    # 🔥 НОВЫЕ
    if new_items:
        block = ["🔥 <b>Новые товары в акциях:</b>\n"]
        for item in new_items:
            title = item.get("product_name") or f"Product {item['product_id']}"
            sku_line = f"  SKU: {item.get('sku')}\n" if item.get("sku") is not None else ""
            action_title = item.get("action_title") or f"Action {item.get('action_id')}"
            block.append(
                f"• {title}\n"
                f"{sku_line}"
                f"  Акция: {action_title}\n"
                f"  {item.get('price')} → {item.get('action_price')} ₽\n"
            )
        messages.append("\n".join(block))
    # 🚨 МЕНЬШЕ МИНИМАЛЬНОЙ ЦЕНЫ
    if new_below_min_items or changed_below_min_items:
        block = ["@eugenius_lesh @grechka37\n", "🚨 <b>Ниже минимальной цены Ozon:</b>\n"]

        for item in new_below_min_items:
            title = item.get("product_name") or f"Product {item['product_id']}"
            sku_line = f"  SKU: {item.get('sku')}\n" if item.get("sku") is not None else ""
            action_title = item.get("action_title") or f"Action {item.get('action_id')}"

            block.append(
                f"• {title}\n"
                f"{sku_line}"
                f"  Акция: {action_title}\n"
                f"  Текущая цена: {item.get('effective_price')} ₽\n"
                f"  Минималка Ozon: {item.get('min_price')} ₽\n"
                f"  Ниже на: {item.get('below_by')} ₽\n"
            )

        for prev, cur in changed_below_min_items:
            title = cur.get("product_name") or f"Product {cur['product_id']}"
            sku_line = f"  SKU: {cur.get('sku')}\n" if cur.get("sku") is not None else ""
            action_title = cur.get("action_title") or f"Action {cur.get('action_id')}"

            changes = []
            for field, label in [
                ("effective_price", "Текущая цена"),
                ("min_price", "Минималка Ozon"),
                ("below_by", "Ниже на"),
            ]:
                if prev.get(field) != cur.get(field):
                    changes.append(f"  {label}: {prev.get(field)} → {cur.get(field)}")

            if not changes:
                continue

            block.append(
                f"• {title}\n"
                f"{sku_line}"
                f"  Акция: {action_title}\n"
                f"{chr(10).join(changes)}\n"
            )

        if len(block) > 2:
            messages.append("\n".join(block))

    # ⚠️ ИЗМЕНЕНИЯ
    if changed_items:
        block = ["⚠️ <b>Изменения в акциях:</b>\n"]
        for prev, cur in changed_items:
            title = cur.get("product_name") or f"Product {cur['product_id']}"
            sku_line = f"  SKU: {cur.get('sku')}\n" if cur.get("sku") is not None else ""
            action_title = cur.get("action_title") or f"Action {cur.get('action_id')}"

            changes = []

            for field, label in [
                ("price", "Цена"),
                ("action_price", "Акционная цена"),
                ("current_boost", "Буст"),
                ("stock", "Остаток"),
                ("add_mode", "Режим"),
            ]:
                if prev.get(field) != cur.get(field):
                    changes.append(f"  {label}: {prev.get(field)} → {cur.get(field)}")

            if not changes:
                continue

            block.append(
                f"• {title}\n"
                f"{sku_line}"
                f"  Акция: {action_title}\n"
                f"{chr(10).join(changes)}\n"
            )

        if len(block) > 1:
            messages.append("\n".join(block))

    # ❌ УДАЛЕННЫЕ
    if ended_items:
        block = ["❌ <b>Удалены из акций:</b>\n"]
        for item in ended_items:
            title = item.get("product_name") or f"Product {item['product_id']}"
            sku_line = f"  SKU: {item.get('sku')}\n" if item.get("sku") is not None else ""
            action_title = item.get("action_title") or f"Action {item.get('action_id')}"
            block.append(
                f"• {title}\n"
                f"{sku_line}"
                f"  Акция завершена: {action_title}\n"
            )
        messages.append("\n".join(block))

    # 🚀 отправка
    for msg in messages:
        for chunk in chunk_text(msg):
            send_telegram_message(chunk)

    save_snapshot(current_items)
    upsert_state(current_items)
    upsert_below_min_state(below_min_items)

    print("✅ ozon alerts check done")

def load_products_map() -> Dict[int, Dict[str, Any]]:
    sql = """
        SELECT sku, name
        FROM public.products
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    result: Dict[int, Dict[str, Any]] = {}
    for sku, name in rows:
        result[int(sku)] = {
            "sku": int(sku),
            "name": name,
        }
    return result

def load_product_id_map() -> Dict[int, Dict[str, Any]]:
    sql = """
        SELECT product_id, sku, ozon_name, offer_id, min_price
        FROM public.ozon_product_mapping
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    result: Dict[int, Dict[str, Any]] = {}
    for product_id, sku, ozon_name, offer_id, min_price in rows:
        result[int(product_id)] = {
            "sku": int(sku) if sku is not None else None,
            "ozon_name": ozon_name,
            "offer_id": offer_id,
            "min_price": float(min_price) if min_price is not None else None,
        }
    return result

def upsert_product_mapping(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    sql = """
        INSERT INTO public.ozon_product_mapping (
            product_id,
            sku,
            ozon_name,
            offer_id,
            min_price,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (product_id)
        DO UPDATE SET
            sku = EXCLUDED.sku,
            ozon_name = EXCLUDED.ozon_name,
            offer_id = EXCLUDED.offer_id,
            min_price = EXCLUDED.min_price,
            updated_at = now()
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    sql,
                    (
                        r["product_id"],
                        r["sku"],
                        r.get("ozon_name"),
                        r.get("offer_id"),
                        r.get("min_price"),
                    ),
                )
        conn.commit()


def enrich_missing_product_mapping(product_ids: List[int]) -> None:
    product_id_map = load_product_id_map()
    missing_ids = sorted({int(x) for x in product_ids if int(x) not in product_id_map})

    if not missing_ids:
        return

    client = get_default_seller_client()
    rows_to_upsert: List[Dict[str, Any]] = []

    chunk_size = 100
    for i in range(0, len(missing_ids), chunk_size):
        chunk = missing_ids[i:i + chunk_size]
        resp = client.get_products_info(chunk)
        items = resp.get("items", []) or []

        for item in items:
            product_id = item.get("id") or item.get("product_id")
            sku = item.get("sku")

            if product_id is None or sku is None:
                continue

            min_price_raw = item.get("min_price")
            min_price = float(min_price_raw) if min_price_raw not in (None, "", "0", "0.00") else None

            rows_to_upsert.append(
                {
                    "product_id": int(product_id),
                    "sku": int(sku),
                    "ozon_name": item.get("name"),
                    "offer_id": item.get("offer_id"),
                    "min_price": min_price,
                }
            )

    upsert_product_mapping(rows_to_upsert)

def collect_below_min_price_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []

    for item in items:
        min_price = item.get("min_price")
        if min_price is None:
            continue

        current_price = item.get("action_price")
        if current_price is None:
            current_price = item.get("price")

        if current_price is None:
            continue

        try:
            current_price_f = float(current_price)
            min_price_f = float(min_price)
        except (TypeError, ValueError):
            continue

        if current_price_f < min_price_f:
            result.append(
                {
                    **item,
                    "effective_price": current_price_f,
                    "min_price": min_price_f,
                    "below_by": round(min_price_f - current_price_f, 2),
                }
            )

    return result

def make_below_min_key(item: Dict[str, Any]) -> str:
    return f"below_min:action:{item['action_id']}:product:{item['product_id']}"


def normalize_below_min_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "action_id": item["action_id"],
        "action_title": item.get("action_title"),
        "product_id": item["product_id"],
        "sku": item.get("sku"),
        "product_name": item.get("product_name"),
        "offer_id": item.get("offer_id"),
        "effective_price": float(item["effective_price"]) if item.get("effective_price") is not None else None,
        "min_price": float(item["min_price"]) if item.get("min_price") is not None else None,
        "below_by": float(item["below_by"]) if item.get("below_by") is not None else None,
    }


def load_below_min_state() -> Dict[str, Dict[str, Any]]:
    sql = """
        SELECT key, payload_json
        FROM public.ozon_below_min_state
        WHERE is_active = true
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    result: Dict[str, Dict[str, Any]] = {}
    for key, payload_json in rows:
        result[key] = payload_json
    return result


def diff_below_min_states(
    current_items: List[Dict[str, Any]],
    previous_state: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Tuple[Dict[str, Any], Dict[str, Any]]], List[Dict[str, Any]]]:
    current_map = {make_below_min_key(x): normalize_below_min_item(x) for x in current_items}

    new_items: List[Dict[str, Any]] = []
    changed_items: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    ended_items: List[Dict[str, Any]] = []

    for key, cur in current_map.items():
        prev = previous_state.get(key)
        if prev is None:
            new_items.append(cur)
        elif prev != cur:
            changed_items.append((prev, cur))

    for key, prev in previous_state.items():
        if key not in current_map:
            ended_items.append(prev)

    return new_items, changed_items, ended_items


def upsert_below_min_state(items: List[Dict[str, Any]]) -> None:
    current_map = {make_below_min_key(x): normalize_below_min_item(x) for x in items}
    current_keys = set(current_map.keys())

    with get_connection() as conn:
        with conn.cursor() as cur:
            for key, payload in current_map.items():
                cur.execute(
                    """
                    INSERT INTO public.ozon_below_min_state (
                        key, payload_json, first_seen_at, last_seen_at, is_active
                    )
                    VALUES (%s, %s::jsonb, now(), now(), true)
                    ON CONFLICT (key)
                    DO UPDATE SET
                        payload_json = EXCLUDED.payload_json,
                        last_seen_at = now(),
                        is_active = true
                    """,
                    (key, json.dumps(payload, ensure_ascii=False)),
                )

            cur.execute("SELECT key FROM public.ozon_below_min_state WHERE is_active = true")
            existing_keys = {row[0] for row in cur.fetchall()}

            to_disable = existing_keys - current_keys
            for key in to_disable:
                cur.execute(
                    """
                    UPDATE public.ozon_below_min_state
                    SET is_active = false, last_seen_at = now()
                    WHERE key = %s
                    """,
                    (key,),
                )

        conn.commit()