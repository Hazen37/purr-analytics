# Purr Analytics — ETL и аналитика Ozon

Проект загружает данные из **Ozon Seller API** и **Performance API** в **PostgreSQL**, строит агрегаты для **Metabase** и содержит вспомогательные CLI (отладка API, маппинг товаров, Telegram-алерты по акциям).

Принцип работы — **идемпотентный ETL** с **окном lookback** (`ETL_LOOKBACK_DAYS`): повторные запуски не должны плодить дубликаты и ломать агрегаты.

---

## Назначение и поток данных

| Источник | Что грузится |
|----------|----------------|
| Seller API | FBO-отправления (заказы), позиции, клиенты, комиссии из posting, финансы (Finance API), остатки, отзывы, метрики воронки по SKU/дню |
| Performance API | Кампании, дневная статистика, атрибуция заказов к рекламе |
| Локальный справочник | `src/catalog/product_catalog.py` — вкус/граммы по SKU для связки с `products` |

**Metabase** подключается к той же БД `purr` (в типовом `docker-compose`).

---

## Структура репозитория

```
purr-analytics/
├── docker-compose.yml      # postgres + metabase + etl_runner (разовый прогон ETL)
├── Dockerfile
├── requirements.txt
├── rebuild.sh              # удобная обёртка: build + run etl_runner update_all
├── deploy/
│   ├── initdb/
│   │   └── 01-create-metabase.sql   # CREATE DATABASE metabase
│   └── sql/
│       └── orders_clean.sql         # (опционально) старая схема view
│
└── src/
    ├── cli/
    │   ├── update_all.py            # главная точка входа ETL
    │   ├── run_ozon_alerts.py       # Telegram-алерты по акциям
    │   ├── fill_product_mapping.py / rebuild_product_mapping.py / refresh_mapping_min_price.py
    │   └── debug_ozon_*.py          # отладка цен, акций, карточки товара
    │
    ├── core/
    │   ├── config.py                # .env, ключи Ozon, БД
    │   └── db.py                    # psycopg2, execute_query / fetch_*
    │
    ├── migrations/
    │   └── run.py                   # идемпотентные DDL
    │
    ├── catalog/
    │   └── product_catalog.py       # SKU → flavor, grams (под магазин)
    │
    ├── ozon/
    │   ├── seller_api.py            # OzonSellerClient
    │   ├── analytics_api.py         # отчёты / аналитика (воронка SKU)
    │   ├── performance_api.py
    │   └── reports_api.py
    │
    ├── etl/
    │   ├── orders/load_orders.py
    │   ├── finance/finance_api.py, period_costs.py
    │   ├── performance/campaigns.py, daily.py, orders.py
    │   ├── reviews/load_reviews.py
    │   ├── analytics/sku_day_metrics.py
    │   ├── stocks/load_stocks.py
    │   └── alerts/ozon_discounts_alerts.py   # акции + Telegram
```

---

## Архитектура ETL

### Принципы

- **Идемпотентность**: UPSERT заказов и позиций, `order_fee_items.uid` + уникальный индекс для строк из Finance API.
- **Lookback**: по умолчанию пересчитывается последний `ETL_LOOKBACK_DAYS` календарных дней (границы включительно для строковых дат в финансах/performance).
- **Разделение сущностей**: `orders` / `order_items` / `order_fee_items`, `perf_*`, `finance_period_costs`, `ozon_sku_day_metrics`, `stocks_current`, `ozon_reviews`.

### Пайплайн `update_all` (порядок шагов)

1. Миграции БД.
2. Пересоздание представления **`orders_clean`** (исключение тестового покупателя, см. `TEST_CUSTOMER_ID`).
3. Заказы FBO + товары + клиенты + комиссии из posting.
4. **`recalc_order_numbers()`** в БД — нумерация заказов/групп для не отменённых.
5. Finance API → `order_fee_items`.
6. Пересчёт **`finance_period_costs`**.
7. Опционально **Performance** (только если `ETL_ENABLE_PERFORMANCE=1`).
8. Отзывы (`ETL_ENABLE_REVIEWS`).
9. Метрики воронки по SKU за период (`ozon_sku_day_metrics`, `ETL_ENABLE_ANALYTICS`).
10. Текущие остатки **`stocks_current`** (`ETL_ENABLE_STOCKS`).

Представление **`vw_sku_day_business`** (создаётся в миграциях) объединяет `ozon_sku_day_metrics` с заказами и справочником `products`.

---

## Быстрый старт

### 1. Инфраструктура

```bash
cd purr-analytics
docker compose up -d
```

Сервисы:

- **`purr_postgres`** — PostgreSQL 16, БД `purr`, порт `5432`.
- **`purr_metabase`** — Metabase, порт `3000` (внутренняя БД Metabase — отдельная `metabase`, создаётся скриптом initdb).
- **`etl_runner`** — собирается из `Dockerfile`, по умолчанию один раз выполняет `python -m src.cli.update_all` (`restart: "no"`). Для повторных прогонов используйте `docker compose run` (см. ниже).

### 2. Секреты и `.env`

В корне нужен **`.env`** (не коммитить): как минимум `OZON_CLIENT_ID`, `OZON_API_KEY`. Для Performance: `OZON_PERF_CLIENT_ID`, `OZON_PERF_CLIENT_SECRET`.  
Параметры БД в `docker-compose` для `etl_runner` заданы явно (`DB_HOST=postgres`, `DB_NAME=purr`); при локальном запуске без Docker выставьте `DB_*` в соответствии с `src/core/config.py` (дефолт имени БД в коде — `ozon_analytics`, для compose используется **`purr`**).

### 3. Миграции

```bash
docker compose run --rm etl_runner python -m src.migrations.run
```

### 4. Запуск ETL

Через обёртку (пересборка образа + прогон):

```bash
./rebuild.sh
```

Или вручную:

```bash
docker compose run --rm etl_runner sh -lc "ETL_LOOKBACK_DAYS=30 python -m src.cli.update_all"
```

С явным диапазоном дат:

```bash
docker compose run --rm etl_runner python -m src.cli.update_all 2025-10-01 2025-12-31
```

---

## Переменные окружения ETL

| Переменная | Смысл |
|------------|--------|
| `ETL_LOOKBACK_DAYS` | Глубина окна, если даты не заданы в CLI (по умолчанию `30`). |
| `ETL_ENABLE_PERFORMANCE` | `1` — включить блок Performance (кампании, дневная статистика, атрибуция). По умолчанию **`0`**. |
| `ETL_ENABLE_PERF_CAMPAIGNS` / `ETL_ENABLE_PERF_DAILY` / `ETL_ENABLE_PERF_ORDERS` | Уточнение шагов Performance при `ETL_ENABLE_PERFORMANCE=1` (по умолчанию все `1`). |
| `ETL_ENABLE_REVIEWS` | Загрузка отзывов (по умолчанию `1`). |
| `ETL_ENABLE_ANALYTICS` | `ozon_sku_day_metrics` (по умолчанию `1`). |
| `ETL_ENABLE_STOCKS` | `stocks_current` (по умолчанию `1`). |
| `ETL_STRICT_MODE` | `1` — падать при ошибке на любом шаге; `0` — продолжать после некритичных шагов (кроме обязательных: миграции, заказы). |
| `ETL_LOG_TIME_UTC` | `1` — время в логах в UTC (по умолчанию). |
| `TEST_CUSTOMER_ID` | ID покупателя, исключаемого из `orders_clean` (дефолт в коде задан под тестовый аккаунт). |

---

## Ключевые таблицы

| Таблица / объект | Назначение |
|------------------|------------|
| `orders`, `order_items`, `customers` | Факты заказов и позиций |
| `products` | SKU, имя, flavor, grams (из ETL + каталог) |
| `order_fee_items` | Детализация комиссий; `source`: `posting_financial`, `finance_api`; `uid` для UPSERT |
| `perf_campaigns`, `performance_campaign_daily`, `performance_order_attribution` | Реклама Performance |
| `finance_period_costs` | Агрегаты расходов по дням (`cost_date`, `fee_group`, `fee_name`) |
| `ozon_reviews` | Отзывы |
| `ozon_sku_day_metrics` | Воронка по SKU (impressions, views, cart_adds, …) |
| `stocks_current` | Остатки по складам |
| `orders_clean` | VIEW над `orders` без тестового `customer_id` + поле `delivery_cluster` |

Таблицы для **Telegram-алертов по акциям** (`ozon_alert_state`, `ozon_product_mapping`, `ozon_action_products_snapshot`, `ozon_below_min_state`) используются модулем `etl/alerts`; при необходимости их DDL нужно добавить в миграции или создать вручную перед первым запуском `run_ozon_alerts`.

---

## Metabase

Подключение к хосту `postgres` (или `localhost` с хоста), БД `purr`, пользователь/пароль как в compose.

Рекомендации для запросов:

- фильтровать по `order_date` / датам метрик;
- недельные агрегации через `date_trunc('week', …)`;
- для временных рядов обрезать «хвост» из нулевых периодов справа, если нужно.

---

## Дополнительные CLI

- **`python -m src.cli.run_ozon_alerts`** — проверка акций, снимки, уведомления в Telegram. Нужны `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` и таблицы состояния в БД.
- **`fill_product_mapping` / `rebuild_product_mapping` / `refresh_mapping_min_price`** — обслуживание `ozon_product_mapping` для алертов.
- **`debug_ozon_prices`**, **`debug_ozon_actions`**, **`debug_ozon_product_info`** — отладка ответов API.

---

## Проверки после ETL

```sql
SELECT COUNT(*) FROM order_fee_items WHERE source = 'finance_api';
SELECT MIN(order_date), MAX(order_date) FROM orders;
SELECT stat_date, SUM(spend)
FROM performance_campaign_daily
GROUP BY 1 ORDER BY 1 DESC;
```

---

## Отладка

Логи контейнера, который выполнял ETL:

```bash
docker compose logs etl_runner
```

PostgreSQL:

```bash
docker exec -it purr_postgres psql -U postgres -d purr
```

---

## Возможные доработки

- Отдельный long-running сервис/cron для регулярного ETL вместо разового `etl_runner`.
- Вынести DDL таблиц алертов в `migrations/run.py`.
- Healthcheck CLI, bulk COPY, более инкрементальный Finance API.
- Автоматические data tests (объёмы строк, свежесть данных).
