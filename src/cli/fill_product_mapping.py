from src.ozon.seller_api import get_default_seller_client
from src.core.db import get_connection


def get_unknown_product_ids():
    sql = """
        SELECT DISTINCT (payload_json->>'product_id')::bigint
        FROM public.ozon_alert_state s
        LEFT JOIN public.ozon_product_mapping m
            ON (payload_json->>'product_id')::bigint = m.product_id
        WHERE m.product_id IS NULL
          AND payload_json ? 'product_id'
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [row[0] for row in cur.fetchall()]


def save_mapping(rows):
    sql = """
        INSERT INTO public.ozon_product_mapping (product_id, sku)
        VALUES (%s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET
            sku = EXCLUDED.sku,
            updated_at = now()
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(sql, (r["product_id"], r["sku"]))
        conn.commit()


def main():
    client = get_default_seller_client()

    product_ids = get_unknown_product_ids()

    if not product_ids:
        print("✅ всё уже замаплено")
        return

    print(f"🔍 нашли {len(product_ids)} новых product_id")

    chunk_size = 100
    all_rows = []

    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i:i + chunk_size]

        resp = client.get_products_info(chunk)
        items = resp.get("items", []) or []

        for item in items:
            product_id = item.get("id") or item.get("product_id")
            sku = item.get("sku")

            if product_id is None or sku is None:
                continue

            all_rows.append({
                "product_id": int(product_id),
                "sku": int(sku),
            })

    save_mapping(all_rows)

    print(f"✅ сохранено {len(all_rows)} записей")


if __name__ == "__main__":
    main()