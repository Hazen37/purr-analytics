from pprint import pprint
from src.ozon.seller_api import get_default_seller_client


def main():
    client = get_default_seller_client()
    resp = client.get_products_info([2353010522])
    pprint(resp)


if __name__ == "__main__":
    main()