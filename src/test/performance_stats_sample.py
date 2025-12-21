
import os, requests, datetime as dt

BASE = "https://api-performance.ozon.ru"
CLIENT_ID = os.getenv("OZON_PERF_CLIENT_ID")
CLIENT_SECRET = os.getenv("OZON_PERF_CLIENT_SECRET")

def get_token():
    r = requests.post(f"{BASE}/api/client/token", json={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def stats_sample(token):
    to_date = dt.date.today()
    from_date = to_date - dt.timedelta(days=60)

    payload = {
        "dateFrom": str(from_date),
        "dateTo": str(to_date),
        "campaigns": ["18179987"],
        "groupBy": "DATE",  # если не так — API скажет, как правильно
        # "metrics": ["IMPRESSIONS", "CLICKS", "SPENT", "ORDERS"]  # тоже может отличаться
    }

    r = requests.post(
        f"{BASE}/api/client/statistics/json",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=60
    )
    print(r.request.body)
    print("STATUS:", r.status_code)
    print(r.text)

def stats_attr(token, from_date, to_date, campaign_ids):
    payload = {
        "dateFrom": str(from_date),
        "dateTo": str(to_date),
        "campaigns": campaign_ids,
        "groupBy": "DATE",
    }
    r = requests.post(
        f"{BASE}/api/client/statistics/attribution/json",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=60
    )
    print("campaigns:", campaign_ids, "status:", r.status_code, "body:", r.text[:300])

if __name__ == "__main__":
    t = get_token()
    to_date = dt.date.today()
    from_date = to_date - dt.timedelta(days=60)

    # тест по одной
    stats_attr(t, from_date, to_date, ["18179987"])
    stats_attr(t, from_date, to_date, ["18179988"])

