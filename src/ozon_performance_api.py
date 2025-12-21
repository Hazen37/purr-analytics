import requests
from .config import settings

BASE = "https://performance.ozon.ru"

def get_perf_token() -> str:
    url = f"{BASE}/api/client/token"
    payload = {
        "client_id": str(settings.OZON_PERF_CLIENT_ID),
        "client_secret": str(settings.OZON_PERF_CLIENT_SECRET),
        "grant_type": "client_credentials",
    }
    r = requests.post(url, json=payload, timeout=60, headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    r.raise_for_status()
    data = r.json()
    return data["access_token"]

if __name__ == "__main__":
    token = get_perf_token()
    print("OK, token starts with:", token[:25])
