# Purr Analytics

ETL + аналитика Ozon (финансы, комиссии, продвижение).

## Установка
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

## Переменные окружения
Создать файл .env:
```ini
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ozon_analytics
DB_USER=postgres
DB_PASSWORD=***

OZON_CLIENT_ID=***
OZON_API_KEY=***

OZON_PERF_CLIENT_ID=***
OZON_PERF_CLIENT_SECRET=***

## Запуск
```bash
python -m src.update_all 2025-10-01 2025-12-17

(достаточно 👍)

---

## ШАГ 6. Первый коммит

```bash
git add .
git commit -m "Initial ETL and analytics structure"