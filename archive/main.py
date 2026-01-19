# src/main.py

from datetime import datetime, timedelta

from .config import settings
from .db import fetch_one
from .ozon_seller_api import get_default_seller_client
from .etl import load_fbo_orders_last_n_days

from .metrics import get_summary, get_retention_distribution, get_cohort_by_first_order_month
from datetime import datetime, timedelta

def test_db():
    print("DB host:", settings.DB_HOST)
    print("DB name:", settings.DB_NAME)
    print("DB user:", settings.DB_USER)

    row = fetch_one("SELECT version();")
    print("PostgreSQL version:", row[0])


def test_ozon_seller_fbo():
    client = get_default_seller_client()

    date_to = datetime.utcnow()
    date_from = date_to - timedelta(days=7)

    print(f"Запрашиваем FBO-отправления с {date_from} по {date_to}...")

    postings = client.get_postings_fbo(date_from=date_from, date_to=date_to, limit=50)

    print(f"Получено FBO-отправлений: {len(postings)}")

    for i, p in enumerate(postings[:2], start=1):
        print(f"\nПример отправления #{i}:")
        print("posting_number:", p.get("posting_number"))
        print("status:", p.get("status"))
        print("in_process_at:", p.get("in_process_at"))
        print("products count:", len(p.get("products", [])))


def main():
    print("=== Тест подключения к БД ===")
    test_db()

    print("\n=== Тест Ozon Seller API (FBO) ===")
    test_ozon_seller_fbo()

    print("\n=== ETL: загрузка заказов за последние 30 дней ===")
    load_fbo_orders_last_n_days(30)

    print("\n=== Тест метрик ===")
    test_metrics()
    
    print("\n=== Тест граммовки и вкуса ===")
    now = datetime.utcnow()
    
    summary_chicken_1500 = get_summary(
        date_from=now - timedelta(days=30),
        date_to=now,
        flavor="Индейка",
        grams=1500,
    )

    print(summary_chicken_1500)
    
def test_metrics():
    """
    Простой тестовый вывод метрик в консоль.
    Сейчас считаем по всем данным без фильтров.
    """
    print("\n=== Метрики по всем заказам ===")
    summary = get_summary()
    for k, v in summary.items():
        print(f"{k}: {v}")

    print("\n=== Распределение клиентов по числу заказов ===")
    retention = get_retention_distribution()
    for row in retention:
        print(f"{row['orders_count']} заказ(ов): {row['customers_count']} клиент(ов)")

    print("\n=== Когорты по месяцу первого заказа ===")
    cohorts = get_cohort_by_first_order_month()
    for row in cohorts:
        print(
            f"{row['cohort_month']}: клиентов={row['customers_count']}, "
            f"выручка={row['cohort_revenue']}, "
            f"средний LTV={row['avg_revenue_per_customer']}"
        )

if __name__ == "__main__":
    main()
