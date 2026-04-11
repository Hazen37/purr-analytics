# src/config.py

"""
Модуль для загрузки конфигурации проекта.

Задачи:
- Прочитать переменные окружения из .env файла.
- Предоставить удобный доступ к настройкам (БД, ключи API).
"""

import os
from dotenv import load_dotenv

# Загружаем переменные из файла .env.
# Поддерживаем два сценария:
# 1) .env в корне проекта
# 2) legacy: .env внутри src/
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
ENV_CANDIDATES = [
    os.path.join(PROJECT_ROOT, ".env"),
    os.path.join(SRC_DIR, ".env"),
]

for env_path in ENV_CANDIDATES:
    load_dotenv(dotenv_path=env_path, override=False)


class Settings:
    """
    Класс-обёртка для настроек.
    Это удобнее, чем каждый раз дергать os.getenv напрямую.
    """

    # Настройки Ozon Seller API
    OZON_CLIENT_ID: str = os.getenv("OZON_CLIENT_ID", "")
    OZON_API_KEY: str = os.getenv("OZON_API_KEY", "")

    # Настройки Ozon Performance API
    OZON_PERF_CLIENT_ID: str = os.getenv("OZON_PERF_CLIENT_ID", "")
    OZON_PERF_CLIENT_SECRET: str = os.getenv("OZON_PERF_CLIENT_SECRET", "")

    # Настройки Yandex Market API
    YANDEXMARKET_API_KEY: str = os.getenv("YANDEXMARKET_API_KEY", "")
    YANDEXMARKET_CAMPAIGN_ID: str = os.getenv("YANDEXMARKET_CAMPAIGN_ID", "")
    YANDEXMARKET_BUSINESS_ID: str = os.getenv("YANDEXMARKET_BUSINESS_ID", "")
    YANDEXMARKET_WAREHOUSE_ID: str = os.getenv("YANDEXMARKET_WAREHOUSE_ID", "")

    # Настройки Wildberries API
    WILDBERRIES_API_KEY: str = os.getenv("WILDBERRIES_API_KEY", "")
    WILDBERRIES_BASE_URL: str = os.getenv("WILDBERRIES_BASE_URL", "https://marketplace-api.wildberries.ru")
    WILDBERRIES_COMMUNICATION_BASE_URL: str = os.getenv(
        "WILDBERRIES_COMMUNICATION_BASE_URL", "https://feedbacks-api.wildberries.ru"
    )
    WILDBERRIES_ANALYTICS_BASE_URL: str = os.getenv(
        "WILDBERRIES_ANALYTICS_BASE_URL", "https://seller-analytics-api.wildberries.ru"
    )

    # Флаги включения маркетплейсов
    ETL_ENABLE_OZON: bool = os.getenv("ETL_ENABLE_OZON", "1") == "1"
    ETL_ENABLE_YANDEXMARKET: bool = os.getenv("ETL_ENABLE_YANDEXMARKET", "0") == "1"
    ETL_ENABLE_WILDBERRIES: bool = os.getenv("ETL_ENABLE_WILDBERRIES", "0") == "1"

    # Настройки БД PostgreSQL
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "ozon_analytics")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")

    @classmethod
    def validate(cls):
        """
        Простая проверка, что самые важные переменные заданы.
        Можно расширить по вкусу.
        """
        missing = []

        if cls.ETL_ENABLE_OZON:
            if not cls.OZON_CLIENT_ID:
                missing.append("OZON_CLIENT_ID")
            if not cls.OZON_API_KEY:
                missing.append("OZON_API_KEY")

        if cls.ETL_ENABLE_YANDEXMARKET:
            if not cls.YANDEXMARKET_API_KEY:
                missing.append("YANDEXMARKET_API_KEY")
            if not cls.YANDEXMARKET_CAMPAIGN_ID:
                missing.append("YANDEXMARKET_CAMPAIGN_ID")

        if cls.ETL_ENABLE_WILDBERRIES:
            if not cls.WILDBERRIES_API_KEY:
                missing.append("WILDBERRIES_API_KEY")

        if missing:
            # Это не жёсткая ошибка, но хорошее предупреждение.
            # На продуктиве можно бросать Exception.
            print(
                f"[config] ВНИМАНИЕ: не заданы переменные: {', '.join(missing)}. "
                f"Проверьте файл .env"
            )


# Создаём единый объект настроек, который будем импортировать в других модулях.
settings = Settings()
settings.validate()