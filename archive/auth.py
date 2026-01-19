import time
import requests

class PerformanceAuth:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expires_at = 0

    def get_token(self) -> str:
        if self.token and time.time() < self.expires_at - 60:
            return self.token

        r = requests.post(
            "https://api-performance.ozon.ru/api/client/token",
            json={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=30,
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        self.expires_at = time.time() + int(data["expires_in"])
        return self.token
