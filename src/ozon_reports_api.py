# src/ozon_reports_api.py

import time
import requests

from .config import settings

BASE_URL = "https://api-seller.ozon.ru"


class OzonReportsClient:
    def __init__(self):
        self.headers = {
            "Client-Id": settings.OZON_CLIENT_ID,
            "Api-Key": settings.OZON_API_KEY,
            "Content-Type": "application/json",
        }

    def create_accruals_report(self, date_from: str, date_to: str) -> str:
        """
        Создаём отчёт по начислениям.
        Возвращает report_code.
        """
        url = f"{BASE_URL}/v1/report/accruals/create"
        payload = {
            "date_from": date_from,
            "date_to": date_to,
        }

        r = requests.post(url, json=payload, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()["result"]["report_code"]

    def wait_report_ready(self, report_code: str, timeout_sec: int = 300) -> str:
        """
        Ждём, пока отчёт будет готов, и возвращаем file_url.
        """
        url = f"{BASE_URL}/v1/report/info"
        start = time.time()

        while True:
            r = requests.post(url, json={"code": report_code}, headers=self.headers, timeout=30)
            r.raise_for_status()

            result = r.json()["result"]
            status = result["status"]

            if status == "DONE":
                return result["file"]

            if status == "FAILED":
                raise RuntimeError("Отчёт Ozon не сформировался")

            if time.time() - start > timeout_sec:
                raise TimeoutError("Таймаут ожидания отчёта Ozon")

            time.sleep(5)

    def download_file(self, file_url: str, path: str):
        """
        Скачиваем файл отчёта.
        """
        r = requests.get(file_url, timeout=60)
        r.raise_for_status()

        with open(path, "wb") as f:
            f.write(r.content)
