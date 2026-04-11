from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from src.core.config import settings
from src.core.db import execute_query
from src.yandex_market.seller_api import get_default_client


def _dt(value: Any) -> datetime | None:
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("feedbacks"), list):
        return payload["feedbacks"]
    if isinstance(payload.get("reviews"), list):
        return payload["reviews"]
    result = payload.get("result") or {}
    if isinstance(result.get("feedbacks"), list):
        return result["feedbacks"]
    if isinstance(result.get("reviews"), list):
        return result["reviews"]
    if isinstance(result.get("items"), list):
        return result["items"]
    return []


def load_reviews(limit: int = 200) -> None:
    client = get_default_client()
    campaign_id = settings.YANDEXMARKET_CAMPAIGN_ID
    business_id = settings.YANDEXMARKET_BUSINESS_ID

    candidates = [
        (
            "POST",
            f"/v2/businesses/{business_id}/goods-feedback",
            {},
            {"reactionStatus": "ALL", "limit": min(limit, 50)},
        ),
        ("GET", f"/v1/campaigns/{campaign_id}/feedback/reviews", {"limit": limit}, None),
        ("GET", f"/v1/businesses/{business_id}/reviews", {"limit": limit}, None),
    ]

    rows: List[Dict[str, Any]] = []
    got_success_response = False
    for method, path, params, payload in candidates:
        try:
            response_payload = client.request(method, path, params=params, payload=payload)
            got_success_response = True
            rows = _extract_rows(response_payload)
            if rows or method == "POST":
                break
        except Exception as exc:
            print(f"[ym_reviews] warn: {path} failed: {exc}")
            if got_success_response:
                break

    if not rows:
        print("[ym_reviews] no rows, skip")
        return

    for review in rows:
        review_id = str(review.get("id") or review.get("reviewId") or "")
        if not review_id:
            continue
        execute_query(
            """
            INSERT INTO ym_reviews (
                review_id, order_id, rating, review_text, published_at, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (review_id) DO UPDATE
            SET order_id = EXCLUDED.order_id,
                rating = EXCLUDED.rating,
                review_text = EXCLUDED.review_text,
                published_at = EXCLUDED.published_at,
                updated_at = now(),
                raw = EXCLUDED.raw;
            """,
            (
                review_id,
                str(review.get("orderId") or ""),
                review.get("rating") or review.get("averageGrade"),
                review.get("text") or review.get("comment") or review.get("content"),
                _dt(
                    review.get("creationDate")
                    or review.get("createdAt")
                    or review.get("publishedAt")
                    or review.get("updatedAt")
                ),
                json.dumps(review, ensure_ascii=False),
            ),
        )

    print(f"[ym_reviews] upserted={len(rows)}")
