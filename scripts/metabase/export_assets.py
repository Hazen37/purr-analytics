#!/usr/bin/env python3
"""
Export Metabase assets to local backup and SQL library folders.

What this script does:
1. Downloads current Metabase collections, dashboards, and cards.
2. Stores a timestamped raw snapshot for rollback/reference.
3. Extracts native SQL questions into a browsable local library.
4. Builds indexes that map dashboard tabs/cards to the underlying SQL.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = BASE_DIR / "metabase"
DEFAULT_METABASE_URL = "http://localhost:3000"


RU_TRANSLIT = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}


@dataclass(frozen=True)
class Usage:
    dashboard_id: int
    dashboard_name: str
    tab_id: int | None
    tab_name: str


def slugify(text: str | None, fallback: str) -> str:
    if not text:
        return fallback
    text = text.strip().lower()
    transliterated: list[str] = []
    for char in text:
        lower_char = char.lower()
        if lower_char in RU_TRANSLIT:
            replacement = RU_TRANSLIT[lower_char]
            if char.isupper():
                replacement = replacement.capitalize()
            transliterated.append(replacement)
        else:
            transliterated.append(char)
    ascii_text = "".join(transliterated)
    ascii_text = ascii_text.encode("ascii", "ignore").decode("ascii")
    ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text)
    ascii_text = re.sub(r"-{2,}", "-", ascii_text).strip("-")
    return ascii_text or fallback


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def ensure_env_loaded() -> None:
    for candidate in (BASE_DIR / ".env", BASE_DIR / "src" / ".env"):
        if candidate.exists():
            load_dotenv(candidate, override=False)


class MetabaseClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def get_json(self, path: str) -> Any:
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            headers={"X-API-Key": self.api_key},
        )
        try:
            with urllib.request.urlopen(req) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Metabase API error for {path}: {exc.code} {detail}") from exc


def get_source_card_id(card: dict[str, Any]) -> int | None:
    dataset_query = card.get("dataset_query") or {}
    for stage in dataset_query.get("stages") or []:
        source_card_id = stage.get("source-card")
        if source_card_id is not None:
            return source_card_id
    legacy_source_card_id = dataset_query.get("source-card-id")
    if legacy_source_card_id is not None:
        return legacy_source_card_id
    return None


def get_native_sql(card: dict[str, Any]) -> str | None:
    dataset_query = card.get("dataset_query") or {}
    native = dataset_query.get("native")
    if isinstance(native, dict):
        query = native.get("query")
        if isinstance(query, str) and query.strip():
            return query.rstrip() + "\n"
    elif isinstance(native, str) and native.strip():
        return native.rstrip() + "\n"

    for stage in dataset_query.get("stages") or []:
        stage_native = stage.get("native")
        if isinstance(stage_native, dict):
            query = stage_native.get("query")
            if isinstance(query, str) and query.strip():
                return query.rstrip() + "\n"
        elif isinstance(stage_native, str) and stage_native.strip():
            return stage_native.rstrip() + "\n"
    return None


def resolve_root_native_cards(
    card_id: int,
    cards_by_id: dict[int, dict[str, Any]],
    seen: set[int] | None = None,
) -> list[int]:
    if seen is None:
        seen = set()
    if card_id in seen:
        return []
    seen.add(card_id)

    card = cards_by_id[card_id]
    if card.get("query_type") == "native" and get_native_sql(card):
        return [card_id]

    source_card_id = get_source_card_id(card)
    if source_card_id and source_card_id in cards_by_id:
        return resolve_root_native_cards(source_card_id, cards_by_id, seen)

    return []


def format_usage(usages: list[Usage]) -> str:
    if not usages:
        return "- not used on dashboards"
    lines = []
    for usage in sorted(
        usages,
        key=lambda item: (item.dashboard_name.lower(), item.tab_name.lower(), item.dashboard_id),
    ):
        lines.append(
            f"- dashboard `{usage.dashboard_name}` (id={usage.dashboard_id}), "
            f"tab `{usage.tab_name}`"
        )
    return "\n".join(lines)


def render_sql_header(
    *,
    card: dict[str, Any],
    collection_name: str,
    usages: list[Usage],
    extracted_at: str,
) -> str:
    lines = [
        "-- Metabase native question export",
        f"-- card_id: {card['id']}",
        f"-- card_name: {card.get('name', '')}",
        f"-- query_type: {card.get('query_type', '')}",
        f"-- display: {card.get('display', '')}",
        f"-- collection: {collection_name}",
        f"-- extracted_at_utc: {extracted_at}",
        "-- dashboard_usage:",
    ]
    if usages:
        for usage in sorted(
            usages,
            key=lambda item: (item.dashboard_name.lower(), item.tab_name.lower(), item.dashboard_id),
        ):
            lines.append(
                f"--   - dashboard `{usage.dashboard_name}` (id={usage.dashboard_id}), "
                f"tab `{usage.tab_name}`"
            )
    else:
        lines.append("--   - not used on dashboards")
    return "\n".join(lines) + "\n\n"


def build_sql_readme(
    *,
    native_cards: list[dict[str, Any]],
    cards_by_id: dict[int, dict[str, Any]],
    usages_by_card_id: dict[int, list[Usage]],
    collection_names: dict[Any, str],
    sql_file_by_card_id: dict[int, str],
    extracted_at: str,
) -> str:
    lines = [
        "# Metabase SQL Library",
        "",
        "Локальная библиотека SQL-запросов, выгруженных из Metabase.",
        "",
        "## Naming",
        "",
        "- `cards/<card_id>__<slug>.sql` - канонический SQL по native question.",
        "- `index.json` - машинно-читаемый индекс по карточкам, зависимостям и использованию на дашбордах.",
        "",
        f"Extracted at (UTC): `{extracted_at}`",
        "",
        "## Native Cards",
        "",
    ]

    for card in sorted(native_cards, key=lambda item: item["id"]):
        lines.extend(
            [
                f"### {card['id']} - {card.get('name', 'Untitled')}",
                "",
                f"- File: `{sql_file_by_card_id[card['id']]}`",
                f"- Collection: `{collection_names.get(card.get('collection_id'), 'Unknown')}`",
                f"- Display: `{card.get('display', '')}`",
                "- Dashboard usage:",
                format_usage(usages_by_card_id.get(card["id"], [])),
                "",
            ]
        )

    lines.extend(["## Derived Cards", ""])
    derived_cards = [
        card for card in cards_by_id.values() if card.get("query_type") != "native"
    ]
    if not derived_cards:
        lines.append("Нет производных карточек.")
    else:
        for card in sorted(derived_cards, key=lambda item: item["id"]):
            root_native_ids = resolve_root_native_cards(card["id"], cards_by_id)
            root_files = [sql_file_by_card_id[root_id] for root_id in root_native_ids if root_id in sql_file_by_card_id]
            lines.extend(
                [
                    f"### {card['id']} - {card.get('name', 'Untitled')}",
                    "",
                    f"- Query type: `{card.get('query_type', '')}`",
                    f"- Source card: `{get_source_card_id(card)}`",
                    (
                        "- Root native SQL: "
                        + ", ".join(f"`{path}`" for path in root_files)
                        if root_files
                        else "- Root native SQL: not resolved"
                    ),
                    "- Dashboard usage:",
                    format_usage(usages_by_card_id.get(card["id"], [])),
                    "",
                ]
            )

    return "\n".join(lines).rstrip() + "\n"


def export_assets(output_dir: Path, base_url: str, api_key: str) -> dict[str, Any]:
    client = MetabaseClient(base_url=base_url, api_key=api_key)
    extracted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    snapshot_name = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    collections = client.get_json("/api/collection")
    dashboards = client.get_json("/api/dashboard")
    cards_list = client.get_json("/api/card")

    collection_names = {
        collection.get("id"): collection.get("name", "Unknown")
        for collection in collections
    }
    dashboards_dir = output_dir / "backups" / snapshot_name / "dashboards"
    cards_dir = output_dir / "backups" / snapshot_name / "cards"
    collections_dir = output_dir / "backups" / snapshot_name / "collections"
    snapshot_dir = output_dir / "backups" / snapshot_name

    write_json(snapshot_dir / "collections.json", collections)
    write_json(snapshot_dir / "dashboards.json", dashboards)
    write_json(snapshot_dir / "cards.json", cards_list)

    for collection in collections:
        collection_id = collection.get("id", "unknown")
        collection_slug = slugify(collection.get("name"), f"collection-{collection_id}")
        write_json(
            collections_dir / f"{collection_id}__{collection_slug}.json",
            collection,
        )

    detailed_dashboards: list[dict[str, Any]] = []
    usages_by_card_id: dict[int, list[Usage]] = defaultdict(list)
    for dashboard in dashboards:
        dashboard_id = dashboard["id"]
        detailed_dashboard = client.get_json(f"/api/dashboard/{dashboard_id}")
        detailed_dashboards.append(detailed_dashboard)
        dashboard_slug = slugify(detailed_dashboard.get("name"), f"dashboard-{dashboard_id}")
        write_json(
            dashboards_dir / f"{dashboard_id}__{dashboard_slug}.json",
            detailed_dashboard,
        )

        tabs = {
            tab["id"]: tab.get("name", "Untitled tab")
            for tab in detailed_dashboard.get("tabs") or []
        }
        for dashcard in detailed_dashboard.get("dashcards") or []:
            card_id = dashcard.get("card_id")
            if not card_id:
                continue
            tab_id = dashcard.get("dashboard_tab_id")
            usages_by_card_id[card_id].append(
                Usage(
                    dashboard_id=dashboard_id,
                    dashboard_name=detailed_dashboard.get("name", f"Dashboard {dashboard_id}"),
                    tab_id=tab_id,
                    tab_name=tabs.get(tab_id, "No tab"),
                )
            )

    cards_by_id: dict[int, dict[str, Any]] = {}
    for card_stub in cards_list:
        card_id = card_stub["id"]
        detailed_card = client.get_json(f"/api/card/{card_id}")
        cards_by_id[card_id] = detailed_card
        card_slug = slugify(detailed_card.get("name"), f"card-{card_id}")
        write_json(cards_dir / f"{card_id}__{card_slug}.json", detailed_card)

    sql_cards_dir = output_dir / "sql" / "cards"
    sql_file_by_card_id: dict[int, str] = {}
    native_cards: list[dict[str, Any]] = []
    for card in sorted(cards_by_id.values(), key=lambda item: item["id"]):
        sql = get_native_sql(card)
        if not sql:
            continue
        native_cards.append(card)
        card_slug = slugify(card.get("name"), f"card-{card['id']}")
        relative_path = Path("cards") / f"{card['id']:03d}__{card_slug}.sql"
        absolute_path = output_dir / "sql" / relative_path
        collection_name = collection_names.get(card.get("collection_id"), "Unknown")
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        absolute_path.write_text(
            render_sql_header(
                card=card,
                collection_name=collection_name,
                usages=usages_by_card_id.get(card["id"], []),
                extracted_at=extracted_at,
            )
            + sql,
            encoding="utf-8",
        )
        sql_file_by_card_id[card["id"]] = relative_path.as_posix()

    sql_index = {
        "extracted_at_utc": extracted_at,
        "native_cards": [],
        "derived_cards": [],
    }
    for card in sorted(cards_by_id.values(), key=lambda item: item["id"]):
        base_entry = {
            "id": card["id"],
            "name": card.get("name"),
            "query_type": card.get("query_type"),
            "display": card.get("display"),
            "collection_id": card.get("collection_id"),
            "collection_name": collection_names.get(card.get("collection_id")),
            "source_card_id": get_source_card_id(card),
            "dashboard_usage": [
                {
                    "dashboard_id": usage.dashboard_id,
                    "dashboard_name": usage.dashboard_name,
                    "tab_id": usage.tab_id,
                    "tab_name": usage.tab_name,
                }
                for usage in usages_by_card_id.get(card["id"], [])
            ],
        }
        if card.get("id") in sql_file_by_card_id:
            base_entry["sql_file"] = sql_file_by_card_id[card["id"]]
            sql_index["native_cards"].append(base_entry)
        else:
            base_entry["root_native_card_ids"] = resolve_root_native_cards(
                card["id"],
                cards_by_id,
            )
            sql_index["derived_cards"].append(base_entry)

    write_json(output_dir / "sql" / "index.json", sql_index)
    (output_dir / "sql" / "README.md").write_text(
        build_sql_readme(
            native_cards=native_cards,
            cards_by_id=cards_by_id,
            usages_by_card_id=usages_by_card_id,
            collection_names=collection_names,
            sql_file_by_card_id=sql_file_by_card_id,
            extracted_at=extracted_at,
        ),
        encoding="utf-8",
    )

    manifest = {
        "extracted_at_utc": extracted_at,
        "snapshot_name": snapshot_name,
        "metabase_url": base_url,
        "counts": {
            "collections": len(collections),
            "dashboards": len(detailed_dashboards),
            "cards": len(cards_by_id),
            "native_sql_cards": len(native_cards),
        },
        "paths": {
            "snapshot_dir": str(snapshot_dir.relative_to(BASE_DIR)),
            "sql_dir": str((output_dir / "sql").relative_to(BASE_DIR)),
            "sql_readme": str((output_dir / "sql" / "README.md").relative_to(BASE_DIR)),
            "sql_index": str((output_dir / "sql" / "index.json").relative_to(BASE_DIR)),
        },
    }
    write_json(snapshot_dir / "manifest.json", manifest)
    write_json(output_dir / "backups" / "latest.json", manifest)

    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Metabase assets and SQL library.")
    parser.add_argument(
        "--metabase-url",
        default=os.getenv("METABASE_URL", DEFAULT_METABASE_URL),
        help=f"Metabase base URL (default: {DEFAULT_METABASE_URL})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output root directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def main() -> int:
    ensure_env_loaded()
    args = parse_args()
    api_key = os.getenv("METABASE_API_KEY", "").strip()
    if not api_key:
        print(
            "METABASE_API_KEY is missing. Put it into .env or export it in the shell.",
            file=sys.stderr,
        )
        return 1

    manifest = export_assets(
        output_dir=Path(args.output_dir),
        base_url=args.metabase_url,
        api_key=api_key,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
