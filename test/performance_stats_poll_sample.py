import requests
from ..config import settings

BASE = "https://api-performance.ozon.ru"

UUID = "bf91821b-84c3-4b88-b1c8-e978ecf63341"

def get_token():
    r = requests.post(f"{BASE}/api/client/token", json={
        "client_id": settings.OZON_PERF_CLIENT_ID,
        "client_secret": settings.OZON_PERF_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]

def try_get(token, path):
    r = requests.get(f"{BASE}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=60)
    print("\nPATH:", path)
    print("STATUS:", r.status_code)
    print("BODY:", r.text[:2000])
    return r

if __name__ == "__main__":
    token = get_token()

    # 1) самые частые варианты
    try_get(token, f"/api/client/statistics/{UUID}")
    try_get(token, f"/api/client/statistics/result/{UUID}")
    try_get(token, f"/api/client/statistics/result?UUID={UUID}")
    try_get(token, f"/api/client/statistics/file/{UUID}")
    try_get(token, f"/api/client/statistics/download/{UUID}")
    try_get(token, f"/api/client/statistics/download?UUID={UUID}")
