from src.ozon.seller_api import get_default_seller_client
from src.core.db import get_connection


def get_existing_product_ids():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT product_id FROM public.ozon_product_mapping ORDER BY product_id")
            return [row[0] for row in cur.fetchall()]


def save_rows(rows):
    sql = """
        UPDATE public.ozon_product_mapping
        SET
            sku = %s,
            ozon_name = %s,
            offer_id = %s,
            min_price = %s,
            updated_at = now()
        WHERE product_id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    sql,
                    (
                        row["sku"],
                        row["ozon_name"],
                        row["offer_id"],
                        row["min_price"],
                        row["product_id"],
                    ),
                )
        conn.commit()


def main():
    client = get_default_seller_client()
    product_ids = get_existing_product_ids()

    all_rows = []
    chunk_size = 100

    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i:i + chunk_size]
        resp = client.get_products_info(chunk)
        items = resp.get("items", []) or []

        for item in items:
            product_id = item.get("id") or item.get("product_id")
            sku = item.get("sku")
            if product_id is None or sku is None:
                continue

            min_price_raw = item.get("min_price")
            min_price = float(min_price_raw) if min_price_raw not in (None, "", "0", "0.00") else None

            all_rows.append(
                {
                    "product_id": int(product_id),
                    "sku": int(sku),
                    "ozon_name": item.get("name"),
                    "offer_id": item.get("offer_id"),
                    "min_price": min_price,
                }
            )

    save_rows(all_rows)
    print(f"✅ updated {len(all_rows)} mapping rows")


if __name__ == "__main__":
    main()