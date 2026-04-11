from pprint import pprint

from src.ozon.seller_api import get_default_seller_client


def main():
    client = get_default_seller_client()

    resp = client.get_product_prices(
        filter_payload={},
        last_id="",
        limit=20,
    )

    print("=== TOP LEVEL KEYS ===")
    print(list(resp.keys()))

    print("\n=== RAW RESPONSE ===")
    pprint(resp)


if __name__ == "__main__":
    main()