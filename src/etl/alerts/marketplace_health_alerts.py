from __future__ import annotations

import os
from typing import List, Tuple

import requests

from src.core.db import fetch_one

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def _send_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[alerts] skip telegram: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are not set")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )
    if not resp.ok:
        raise RuntimeError(f"Telegram error: {resp.text}")


def _metric(sql: str):
    row = fetch_one(sql)
    return row[0] if row and row[0] is not None else 0


def _ozon_section() -> str:
    orders_24h = _metric("SELECT COUNT(*) FROM orders WHERE order_date >= now() - interval '24 hours';")
    low_stock = _metric("SELECT COUNT(*) FROM stocks_current WHERE free_to_sell <= 2;")
    reviews_24h = _metric("SELECT COUNT(*) FROM ozon_reviews WHERE published_at >= now() - interval '24 hours';")
    return (
        "🔵 <b>Ozon</b>\n"
        f"• Заказов за 24ч: {orders_24h}\n"
        # f"• SKU с низким остатком (до 2): {low_stock}\n"
        # f"• Новых отзывов за 24ч: {reviews_24h}"
    )


def _ym_section() -> str:
    orders_24h = _metric("SELECT COUNT(*) FROM ym_orders WHERE order_date >= now() - interval '24 hours';")
    low_stock = _metric("SELECT COUNT(*) FROM ym_stocks_current WHERE COALESCE(fit, 0) <= 2;")
    reviews_24h = _metric("SELECT COUNT(*) FROM ym_reviews WHERE published_at >= now() - interval '24 hours';")
    return (
        "🟠 <b>Yandex Market</b>\n"
        f"• Заказов за 24ч: {orders_24h}\n"
        # f"• SKU с низким остатком (до 2): {low_stock}\n"
        f"• Новых отзывов за 24ч: {reviews_24h}"
    )


def _wb_section() -> str:
    orders_24h = _metric("SELECT COUNT(*) FROM wb_orders WHERE order_date >= now() - interval '24 hours';")
    low_stock = _metric("SELECT COUNT(*) FROM wb_stocks_current WHERE COALESCE(quantity, 0) <= 2;")
    reviews_24h = _metric("SELECT COUNT(*) FROM wb_reviews WHERE published_at >= now() - interval '24 hours';")
    return (
        "🟣 <b>Wildberries</b>\n"
        f"• Заказов за 24ч: {orders_24h}\n"
        # f"• SKU с низким остатком (до 2): {low_stock}\n"
        f"• Новых отзывов за 24ч: {reviews_24h}"
    )


def run_marketplace_health_alerts(
    *,
    include_ozon: bool = True,
    include_ym: bool = True,
    include_wb: bool = True,
    notify_ozon: bool = True,
    notify_ym: bool = True,
    notify_wb: bool = True,
) -> None:
    print("[alerts] marketplace health check")
    sections: List[Tuple[str, str, bool]] = []

    if include_ozon:
        sections.append(("ozon", _ozon_section(), notify_ozon))
    if include_ym:
        sections.append(("yandex_market", _ym_section(), notify_ym))
    if include_wb:
        sections.append(("wildberries", _wb_section(), notify_wb))

    # Разделяем сообщения по площадкам, чтобы в телеграме сразу читалось.
    for platform, msg, should_notify in sections:
        if should_notify:
            _send_message(msg)
        else:
            print(f"[alerts] notify disabled for {platform}")

    print("[alerts] marketplace health alerts done ✅")
