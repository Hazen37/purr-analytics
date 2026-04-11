from src.ozon.seller_api import get_default_seller_client
from src.core.db import get_connection


def get_all_product_ids():
    sql = """
        SELECT DISTINCT (payload_json->>'product_id')::bigint AS product_id
        FROM public.ozon_alert_state
        WHERE payload_json ? 'product_id'

        UNION

        SELECT DISTINCT product_id
        FROM public.ozon_product_mapping

        ORDER BY 1
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [row[0] for row in cur.fetchall()]


def save_mapping(rows):
    sql = """
        INSERT INTO public.ozon_product_mapping (
            product_id,
            sku,
            ozon_name,
            offer_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (product_id)
        DO UPDATE SET
            sku = EXCLUDED.sku,
            ozon_name = EXCLUDED.ozon_name,
            offer_id = EXCLUDED.offer_id,
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
                        r["ozon_name"],
                        r["offer_id"],
                    ),
                )
        conn.commit()


def main():
    client = get_default_seller_client()

    product_ids = get_all_product_ids()

    if not product_ids:
        print("⚠️ нет product_id для пересборки")
        return

    print(f"🔍 нашли {len(product_ids)} product_id для проверки")

    all_rows = []
    chunk_size = 100

    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i:i + chunk_size]
        resp = client.get_products_info(chunk)

        items = resp.get("items", []) or []
        print(f"chunk {i // chunk_size + 1}: requested={len(chunk)}, got={len(items)}")

        for item in items:
            product_id = item.get("id") or item.get("product_id")
            sku = item.get("sku")
            ozon_name = item.get("name")
            offer_id = item.get("offer_id")

            if product_id is None or sku is None:
                continue

            all_rows.append(
                {
                    "product_id": int(product_id),
                    "sku": int(sku),
                    "ozon_name": ozon_name,
                    "offer_id": offer_id,
                }
            )

    print(f"✅ к записи подготовлено {len(all_rows)} строк")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.ozon_product_mapping")
        conn.commit()

    save_mapping(all_rows)

    print("✅ ozon_product_mapping полностью пересобран")


if __name__ == "__main__":
    main()