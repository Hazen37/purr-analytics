import csv
import sys
from decimal import Decimal

from src.core.db import get_connection


VALID_MARKETPLACES = {"ozon", "yandex_market", "wildberries"}
VALID_KEY_TYPES = {"sku", "offer_id", "article", "nm_id"}


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def parse_grams(value: str | None) -> int | None:
    value = clean_text(value)
    if value is None:
        return None
    return int(value)


def parse_confidence(value: str | None) -> Decimal:
    value = clean_text(value)
    return Decimal(value) if value is not None else Decimal("1.0")


def upsert_canonical_product(cur, row: dict[str, str]) -> int:
    canonical_key = clean_text(row.get("canonical_key"))
    canonical_name = clean_text(row.get("canonical_name")) or canonical_key
    flavor = clean_text(row.get("flavor"))
    grams = parse_grams(row.get("grams"))
    source = clean_text(row.get("source")) or "manual_csv"

    if canonical_key is None:
        raise ValueError("canonical_key is required")

    cur.execute(
        """
        INSERT INTO public.canonical_products (
            canonical_key,
            canonical_name,
            flavor,
            grams,
            source,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (canonical_key)
        DO UPDATE SET
            canonical_name = EXCLUDED.canonical_name,
            flavor = EXCLUDED.flavor,
            grams = EXCLUDED.grams,
            source = EXCLUDED.source,
            updated_at = now()
        RETURNING canonical_product_id;
        """,
        (canonical_key, canonical_name, flavor, grams, source),
    )
    result = cur.fetchone()
    if result is None:
        raise RuntimeError(f"Failed to upsert canonical product for key={canonical_key}")
    return int(result[0])


def upsert_marketplace_mapping(cur, row: dict[str, str], canonical_product_id: int) -> None:
    marketplace = clean_text(row.get("marketplace"))
    external_key_type = clean_text(row.get("external_key_type"))
    external_key = clean_text(row.get("external_key"))
    source = clean_text(row.get("source")) or "manual_csv"
    notes = clean_text(row.get("notes"))
    confidence = parse_confidence(row.get("confidence"))

    if marketplace not in VALID_MARKETPLACES:
        raise ValueError(f"Unsupported marketplace: {marketplace!r}")
    if external_key_type not in VALID_KEY_TYPES:
        raise ValueError(f"Unsupported external_key_type: {external_key_type!r}")
    if external_key is None:
        raise ValueError("external_key is required")

    cur.execute(
        """
        INSERT INTO public.marketplace_product_mapping (
            marketplace,
            external_key_type,
            external_key,
            canonical_product_id,
            source,
            confidence,
            notes,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (marketplace, external_key_type, external_key)
        DO UPDATE SET
            canonical_product_id = EXCLUDED.canonical_product_id,
            source = EXCLUDED.source,
            confidence = EXCLUDED.confidence,
            notes = EXCLUDED.notes,
            updated_at = now();
        """,
        (
            marketplace,
            external_key_type,
            external_key,
            canonical_product_id,
            source,
            confidence,
            notes,
        ),
    )


def import_csv(path: str) -> None:
    with open(path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("⚠️ CSV is empty")
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            imported = 0
            for idx, row in enumerate(rows, start=2):
                try:
                    canonical_product_id = upsert_canonical_product(cur, row)
                    upsert_marketplace_mapping(cur, row, canonical_product_id)
                    imported += 1
                except Exception as exc:
                    raise RuntimeError(f"Row {idx}: {exc}") from exc
        conn.commit()

    print(f"✅ imported {imported} mapping rows from {path}")


def main() -> None:
    if len(sys.argv) != 2:
        print(
            "Usage: python -m src.cli.import_marketplace_product_mapping "
            "src/cli/marketplace_product_mapping.example.csv"
        )
        raise SystemExit(1)

    import_csv(sys.argv[1])


if __name__ == "__main__":
    main()
