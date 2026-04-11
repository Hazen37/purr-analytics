#!/bin/bash
set -euo pipefail

PGPASSWORD="postgres"
CONTAINER="purr_etl_cron"
DB_CONTAINER="purr_postgres"
DB="purr"
TEST_CUSTOMER_ID="47533921"

echo "🏗️ Build etl_cron image..."
docker-compose build etl_cron

echo "🚀 Ensure postgres + metabase are up..."
docker-compose up -d postgres metabase

echo "🧹 Remove old etl_cron containers (workaround compose ContainerConfig bug)..."
# удаляем любые контейнеры, созданные из образа etl_cron
ETL_IDS="$(docker ps -a --filter "ancestor=purr-analytics_etl_cron:latest" --format '{{.ID}}' || true)"
if [ -n "${ETL_IDS}" ]; then
  docker rm -f ${ETL_IDS} >/dev/null 2>&1 || true
fi
# и на всякий случай по имени
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true

echo "🧠 Start etl_cron manually (compose is buggy here)..."
# определяем network проекта (обычно purr-analytics_default)
NET="$(docker network ls --format '{{.Name}}' | grep -E 'purr-analytics_default' | head -n 1 || true)"
if [ -z "${NET}" ]; then
  # fallback: любой *_default
  NET="$(docker network ls --format '{{.Name}}' | grep -E '_default$' | head -n 1)"
fi
echo "   using network: ${NET}"

docker run -d --name "${CONTAINER}" --network "${NET}" \
  --env-file /home/deploy/purr-analytics/.env \
  -e DB_HOST="${DB_CONTAINER}" \
  -e DB_PORT="5432" \
  -e DB_NAME="${DB}" \
  -e DB_USER="postgres" \
  -e DB_PASSWORD="${PGPASSWORD}" \
  purr-analytics_etl_cron:latest >/dev/null

docker exec -i "${CONTAINER}" python - <<'PY'
import os, psycopg2

print("DB_HOST=", os.getenv("DB_HOST"))
conn = psycopg2.connect(
  host=os.getenv("DB_HOST"),
  port=os.getenv("DB_PORT","5432"),
  dbname=os.getenv("DB_NAME","purr"),
  user=os.getenv("DB_USER","postgres"),
  password=os.getenv("DB_PASSWORD",""),
)
print("DB connect OK ✅")
conn.close()
PY

echo "🧱 Run migrations..."
docker exec -i "${CONTAINER}" python -m src.migrations.run

echo "🪟 Recreate view orders_clean..."
docker exec -i "${DB_CONTAINER}" psql -U postgres -d "${DB}" -c "
DROP VIEW IF EXISTS public.orders_clean;
CREATE VIEW public.orders_clean AS
SELECT
  o.*,
  COALESCE(o.delivery_city, '(нет города)') || ' / ' || COALESCE(o.warehouse_name, '(нет склада)') AS delivery_cluster
FROM public.orders o
WHERE o.customer_id <> '${TEST_CUSTOMER_ID}';
"

echo "🧮 Recalculate derived fields..."
docker exec -i "${CONTAINER}" python - <<'PY'
from src.etl.orders.load_orders import (
    recalc_order_group_num_delivered,
    recalc_orders_finance,
    recalc_orders_fees_breakdown,
)

recalc_order_group_num_delivered()
recalc_orders_finance()
recalc_orders_fees_breakdown()

print("Recalculated successfully ✅")
PY

echo "🔍 Quick checks..."
docker exec -i "${DB_CONTAINER}" psql -U postgres -d "${DB}" -c "
SELECT
  COUNT(*) AS orders_total,
  COUNT(*) FILTER (WHERE customer_id = '${TEST_CUSTOMER_ID}') AS test_orders_in_orders,
  (SELECT COUNT(*) FROM public.orders_clean) AS orders_clean_total,
  COUNT(*) FILTER (WHERE status='delivered' AND order_group_num_delivered IS NOT NULL) AS delivered_with_group_num
FROM public.orders;
"

echo "✅ DONE"
