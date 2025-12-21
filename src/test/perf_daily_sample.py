import requests
from src.config import settings

BASE = "https://api-performance.ozon.ru"

def token():
    r = requests.post(
        f"{BASE}/api/client/token",
        json={
            "client_id": settings.OZON_PERF_CLIENT_ID,
            "client_secret": settings.OZON_PERF_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

t = token()
headers = {"Authorization": f"Bearer {t}"}

# ⚠️ параметры могут называться чуть иначе в зависимости от версии доки
params = {
    "dateFrom": "2025-10-17",
    "dateTo": "2025-12-17",
}

r = requests.get(f"{BASE}/api/client/statistics/daily/json", headers=headers, params=params, timeout=60)
print("STATUS:", r.status_code)
print(r.text[:2000])
