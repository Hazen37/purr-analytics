# src/ozon_seller_api.py

"""
Модуль для работы с Ozon Seller API.

Здесь мы:
- Собираем базовый клиент для запросов к Ozon.
- Реализуем функцию для получения списка отправлений (postings),
  которые по сути и есть наши заказы, с которыми мы будем работать
  в аналитике.

Важно:
- Ozon Seller API работает по схеме:
  - URL: https://api-seller.ozon.ru
  - Авторизация через заголовки: Client-Id и Api-Key
  - Все запросы — POST с JSON-телом
"""

import requests
from datetime import datetime
from typing import List, Dict, Any, Optional

from .config import settings


class OzonSellerAPIError(Exception):
    """
    Свой тип ошибки для проблем с Ozon Seller API.
    Это удобнее, чем бросать просто Exception — по нему можно
    отдельно обрабатывать ошибки запросов к Ozon.
    """
    pass


class OzonSellerClient:
    """
    Класс-клиент для работы с Ozon Seller API.

    Он инкапсулирует:
    - базовый URL
    - заголовки с ключами
    - общую логику запросов
    """

    BASE_URL = "https://api-seller.ozon.ru"

    def __init__(self, client_id: str, api_key: str):
        """
        Инициализация клиента.

        Параметры:
        - client_id: значение из кабинета Ozon (Client-Id)
        - api_key: значение API-ключа
        """
        self.client_id = client_id
        self.api_key = api_key

        # Заголовки, которые будут отправляться в каждом запросе.
        # Ozon ожидает:
        # - Client-Id
        # - Api-Key
        # - Content-Type: application/json
        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Внутренний метод для отправки POST-запроса к Ozon API.

        Параметры:
        - path: путь после базового URL, например "/v3/posting/fbs/list"
        - payload: словарь с телом запроса, который будет отправлен как JSON

        Возвращает:
        - словарь с распарсенным JSON-ответом

        Если что-то пошло не так:
        - поднимает OzonSellerAPIError с описанием
        """
        url = f"{self.BASE_URL}{path}"

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
        except requests.RequestException as e:
            # Это ошибка уровня сети (нет интернета, DNS, таймаут и т.п.)
            raise OzonSellerAPIError(f"Ошибка сети при обращении к {url}: {e}")

        # Если HTTP-статус не 2xx — считаем, что это ошибка.
        if not response.ok:
            raise OzonSellerAPIError(
                f"Ошибка ответа Ozon API: статус {response.status_code}, "
                f"тело: {response.text}"
            )

        try:
            data = response.json()
        except ValueError:
            # Если Ozon вернул невалидный JSON — тоже ошибка
            raise OzonSellerAPIError(
                f"Не удалось распарсить JSON-ответ от Ozon API: {response.text}"
            )

        return data

    def get_postings_fbo(
        self,
        date_from: datetime,
        date_to: datetime,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Получить список отправлений (postings) за указанный период.

        Ozon Seller API предоставляет несколько типов логистики (FBO, FBS и т.д.).
        В самых базовых сценариях нас интересует endpoint:
        - /v3/posting/fbs/list (если ты работаешь по FBS)
        - /v3/posting/fbo/list (если по FBO)

        Здесь для примера используем FBO.

        Параметры:
        - date_from: начальная дата/время (datetime) — "с какого момента"
        - date_to: конечная дата/время (datetime) — "по какой момент"
        - limit: сколько записей запрашивать за один раз (у Ozon есть ограничения, типично 100)

        Важно:
        - Ozon API использует формат ISO 8601 (YYYY-MM-DDTHH:MM:SSZ) или без Z, в зависимости от метода.
          Мы приведём datetime к строке в нужном формате.

        Возвращает:
        - список словарей, каждый словарь — это одно отправление (posting).
        """

        # Преобразуем datetime в строку формата ISO 8601.
        # Например: "2024-01-01T00:00:00Z"
        since_str = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_str = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Базовый payload для запроса.
        # Поля могут отличаться в зависимости от версии API и типа логистики,
        # но основная идея такая: указать период и сортировку.
        payload = {
            "filter": {
                "since": since_str,
                "to": to_str,
                # Можно добавить фильтры по статусам, складам и т.д.
            },
            "limit": limit,
            "offset": 0,
            "with": {
                # В этом блоке можно указывать, какие дополнительные данные вернуть.
                # Например, "analytics_data": True, "financial_data": True и т.п.
                "analytics_data": True,
                "financial_data": True
            }
        }

        postings: List[Dict[str, Any]] = []
        offset = 0

        while True:
            payload["offset"] = offset

            data = self._post("/v2/posting/fbo/list", payload)

            # Структура ответа зависит от конкретного метода,
            # но обычно внутри есть поле "result" с данными.
            result = data.get("result")
            if result is None:
                raise OzonSellerAPIError(f"Неожиданная структура ответа: {data}")

            # result может быть либо:
            # - списком отправлений: [ {...}, {...}, ... ]
            # - словарём с ключом "postings": {"postings": [ {...}, ... ], "has_next": true }
            if isinstance(result, list):
                batch = result
            else:
                batch = result.get("postings", [])

            if not batch:
                break

            postings.extend(batch)

            # Если вернулось меньше, чем limit — тоже можно завершать.
            if len(batch) < limit:
                break

            # Иначе двигаем offset и продолжаем.
            offset += limit

        return postings
    
    def get_postings_fbs(
        self,
        date_from: datetime,
        date_to: datetime,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Получить список отправлений (postings) за указанный период.

        Ozon Seller API предоставляет несколько типов логистики (FBO, FBS и т.д.).
        В самых базовых сценариях нас интересует endpoint:
        - /v3/posting/fbs/list (если ты работаешь по FBS)
        - /v3/posting/fbo/list (если по FBO)

        Здесь для примера используем FBS. 

        Параметры:
        - date_from: начальная дата/время (datetime) — "с какого момента"
        - date_to: конечная дата/время (datetime) — "по какой момент"
        - limit: сколько записей запрашивать за один раз (у Ozon есть ограничения, типично 100)

        Важно:
        - Ozon API использует формат ISO 8601 (YYYY-MM-DDTHH:MM:SSZ) или без Z, в зависимости от метода.
          Мы приведём datetime к строке в нужном формате.

        Возвращает:
        - список словарей, каждый словарь — это одно отправление (posting).
        """

        # Преобразуем datetime в строку формата ISO 8601.
        # Например: "2024-01-01T00:00:00Z"
        since_str = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_str = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Базовый payload для запроса.
        # Поля могут отличаться в зависимости от версии API и типа логистики,
        # но основная идея такая: указать период и сортировку.
        payload = {
            "filter": {
                "since": since_str,
                "to": to_str,
                # Можно добавить фильтры по статусам, складам и т.д.
            },
            "limit": limit,
            "offset": 0,
            "with": {
                # В этом блоке можно указывать, какие дополнительные данные вернуть.
                # Например, "analytics_data": True, "financial_data": True и т.п.
                "analytics_data": True,
                "financial_data": True
            }
        }

        postings: List[Dict[str, Any]] = []
        offset = 0

        while True:
            payload["offset"] = offset

            data = self._post("/v3/posting/fbs/list", payload)

            # Структура ответа зависит от конкретного метода,
            # но обычно внутри есть поле "result" с данными.
            result = data.get("result")
            if result is None:
                raise OzonSellerAPIError(f"Неожиданная структура ответа: {data}")

            # result может быть либо:
            # - списком отправлений: [ {...}, {...}, ... ]
            # - словарём с ключом "postings": {"postings": [ {...}, ... ], "has_next": true }
            if isinstance(result, list):
                batch = result
            else:
                batch = result.get("postings", [])

            if not batch:
                break

            postings.extend(batch)

            # Если вернулось меньше, чем limit — тоже можно завершать.
            if len(batch) < limit:
                break

            # Иначе двигаем offset и продолжаем.
            offset += limit

        return postings


def get_default_seller_client() -> OzonSellerClient:
    """
    Утилита для создания клиента OzonSellerClient
    на основе настроек из config.py (.env).

    Это удобно, чтобы в других модулях можно было просто написать:
        from .ozon_seller_api import get_default_seller_client
        client = get_default_seller_client()
    """
    return OzonSellerClient(
        client_id=settings.OZON_CLIENT_ID,
        api_key=settings.OZON_API_KEY,
    )
