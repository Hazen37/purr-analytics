# src/etl/reviews/load_reviews.py

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.core.db import execute_query
from src.core.db import fetch_one

from src.ozon.seller_api import get_default_seller_client
from src.ozon.seller_api import OzonSellerAPIError


def _get_last_review_id() -> int | None:
    row = fetch_one("SELECT MAX(review_id) AS max_id FROM ozon_reviews;")
    if not row:
        return None
    return row["max_id"]


def _pick(item: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in item and item[k] is not None:
            return item[k]
    return None


def _map_review(item: Dict[str, Any]) -> Dict[str, Any]:
    # В разных версиях API поля могут называться по-разному.
    # Поэтому мягко пытаемся подобрать варианты.
    review_id = _pick(item, "id", "review_id")

    rating = _pick(item, "rating", "score")
    text = _pick(item, "text", "comment", "review_text")

    published_at = _pick(item, "published_at", "created_at", "date")
    updated_at = _pick(item, "updated_at", "modified_at")

    # published_at/updated_at могут приходить строками ISO
    def _to_ts(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            # пробуем пару популярных форматов
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    pass
            return None
        return None

    return {
        "review_id": int(review_id) if review_id is not None else None,
        "sku": _pick(item, "sku"),
        "product_id": _pick(item, "product_id"),
        "offer_id": _pick(item, "offer_id"),
        "product_name": _pick(item, "product_name", "name"),
        "rating": int(rating) if rating is not None else None,
        "review_text": text,
        "published_at": _to_ts(published_at),
        "updated_at": _to_ts(updated_at),
        "status": _pick(item, "status"),
        "likes_count": _pick(item, "likes_count"),
        "dislikes_count": _pick(item, "dislikes_count"),
        "raw": item,
    }


def load_reviews(limit: int = 100, max_pages: int = 500) -> None:
    client = get_default_seller_client()

    last_id = _get_last_review_id()
    pages = 0
    inserted = 0

    while True:
        pages += 1
        if pages > max_pages:
            break

        try:
            resp = client.get_reviews(limit=limit, last_id=last_id, sort_dir="ASC")
        except OzonSellerAPIError as e:
            # 403 Premium Plus — просто недоступно в твоём тарифе
            msg = str(e)
            if "PermissionDenied" in msg or "premium plus" in msg.lower() or "статус 403" in msg:
                print("[reviews] WARNING: reviews endpoint requires Premium Plus. Skipping reviews загрузку.")
                return
            raise

        result = resp.get("result") or {}
        items: List[Dict[str, Any]] = (
            result.get("items")
            or result.get("reviews")
            or resp.get("items")
            or []
        )

        if not items:
            break

        mapped = []
        for it in items:
            r = _map_review(it)
            if r["review_id"] is None:
                continue
            mapped.append(r)

        for r in mapped:
            # execute_query у тебя уже используется как универсальная функция.
            # Я предполагаю, что она поддерживает параметризацию через %s.
            # Если сейчас не поддерживает — скажешь, подстрою под твою реализацию core/db.py.
            execute_query(
                """
                INSERT INTO ozon_reviews (
                    review_id, sku, product_id, offer_id, product_name,
                    rating, review_text, published_at, updated_at, status,
                    likes_count, dislikes_count, loaded_at, raw
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, now(), %s::jsonb
                )
                ON CONFLICT (review_id) DO UPDATE SET
                    sku = EXCLUDED.sku,
                    product_id = EXCLUDED.product_id,
                    offer_id = EXCLUDED.offer_id,
                    product_name = EXCLUDED.product_name,
                    rating = EXCLUDED.rating,
                    review_text = EXCLUDED.review_text,
                    published_at = EXCLUDED.published_at,
                    updated_at = EXCLUDED.updated_at,
                    status = EXCLUDED.status,
                    likes_count = EXCLUDED.likes_count,
                    dislikes_count = EXCLUDED.dislikes_count,
                    loaded_at = now(),
                    raw = EXCLUDED.raw;
                """,
                params=(
                    r["review_id"], r["sku"], r["product_id"], r["offer_id"], r["product_name"],
                    r["rating"], r["review_text"], r["published_at"], r["updated_at"], r["status"],
                    r["likes_count"], r["dislikes_count"], json.dumps(r["raw"], ensure_ascii=False),
                ),
            )
            inserted += 1

        last_id = max(x["review_id"] for x in mapped)

    print(f"[reviews] loaded rows: {inserted}, pages: {pages}, last_id: {last_id}")