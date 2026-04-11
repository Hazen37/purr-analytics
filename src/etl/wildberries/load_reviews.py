from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from src.core.config import settings
from src.core.db import execute_query
from src.wildberries.seller_api import get_default_client


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
    if isinstance(payload.get("data"), list):
        return payload["data"]
    if isinstance(payload.get("result"), list):
        return payload["result"]
    return []


def load_reviews(limit: int = 200) -> None:
    client = get_default_client()
    comm_base = settings.WILDBERRIES_COMMUNICATION_BASE_URL.rstrip("/")
    candidates = [
        (f"{comm_base}/api/v1/feedbacks", {"isAnswered": "false", "take": min(limit, 500), "skip": 0}),
        (f"{comm_base}/api/v1/feedbacks/archive", {"take": min(limit, 500), "skip": 0}),
    ]

    rows: List[Dict[str, Any]] = []
    for path, params in candidates:
        try:
            payload = client.request("GET", path, params=params)
            rows = _extract_rows(payload)
            if rows:
                break
        except Exception as exc:
            print(f"[wb_reviews] warn: {path} failed: {exc}")

    if not rows:
        print("[wb_reviews] no rows, skip")
        return

    for review in rows:
        review_id = str(review.get("id") or review.get("feedbackId") or "")
        if not review_id:
            continue
        execute_query(
            """
            INSERT INTO wb_reviews (
                review_id, nm_id, rating, review_text, published_at, raw
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (review_id) DO UPDATE
            SET nm_id = EXCLUDED.nm_id,
                rating = EXCLUDED.rating,
                review_text = EXCLUDED.review_text,
                published_at = EXCLUDED.published_at,
                updated_at = now(),
                raw = EXCLUDED.raw;
            """,
            (
                review_id,
                review.get("nmId"),
                review.get("productValuation") or review.get("rating"),
                review.get("text") or review.get("answer"),
                _dt(review.get("createdDate") or review.get("createdAt")),
                json.dumps(review, ensure_ascii=False),
            ),
        )

    print(f"[wb_reviews] upserted={len(rows)}")
