from pprint import pprint
from src.ozon.seller_api import get_default_seller_client


def main():
    client = get_default_seller_client()

    print("=== ACTIONS ===")
    actions = client.get_actions()
    pprint(actions)

    result = actions.get("result", [])

    if not result:
        print("❌ нет акций")
        return

    action = result[0]
    action_id = action.get("id")

    print(f"\n=== PRODUCTS FOR ACTION {action_id} ===")

    products = client.get_action_products(action_id)
    pprint(products)


if __name__ == "__main__":
    main()