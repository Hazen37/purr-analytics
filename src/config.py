# src/config.py

"""
Модуль для загрузки конфигурации проекта.

Задачи:
- Прочитать переменные окружения из .env файла.
- Предоставить удобный доступ к настройкам (БД, ключи API).
"""

import os
from dotenv import load_dotenv

# Загружаем переменные из файла .env (если он есть в корне проекта)
# Файл .env НЕ должен коммититься в репозиторий, там секреты.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

load_dotenv(dotenv_path=ENV_PATH)


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

        if not cls.OZON_CLIENT_ID:
            missing.append("OZON_CLIENT_ID")
        if not cls.OZON_API_KEY:
            missing.append("OZON_API_KEY")

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