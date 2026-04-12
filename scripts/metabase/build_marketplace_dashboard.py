#!/usr/bin/env python3
"""
Build a reviewable marketplace dashboard in Metabase.

The script:
1. Reuses the current OZON dashboard parameters as the filter baseline.
2. Creates or updates a separate dashboard: "PURR Analytics Marketplace".
3. Reuses existing OZON cards and adds new cross-marketplace / YM / WB cards.
4. Writes all SQL used by the new cards to a local folder.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
SQL_ROOT = BASE_DIR / "metabase" / "sql" / "marketplace_dashboard"
SQL_CARDS_DIR = SQL_ROOT / "cards"
TARGET_DASHBOARD_NAME = "PURR Analytics Marketplace"
SOURCE_DASHBOARD_ID = 3
DEFAULT_METABASE_URL = "http://localhost:3000"
DATABASE_ID = 2
OZON_CLONE_MARKER = "[marketplace-ozon-clone:"
GENERATED_CARD_MARKER = "[marketplace-generated:"


def ensure_env_loaded() -> None:
    for candidate in (BASE_DIR / ".env", BASE_DIR / "src" / ".env"):
        if candidate.exists():
            load_dotenv(candidate, override=False)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def append_description_marker(description: str | None, marker: str) -> str:
    if description and description.strip():
        return f"{description.rstrip()}\n{marker}"
    return marker


class MetabaseClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        body = None
        headers = {"X-API-Key": self.api_key}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req) as response:
                raw = response.read()
                if not raw:
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path} failed: {exc.code} {detail}") from exc

    def get(self, path: str) -> Any:
        return self.request("GET", path)

    def post(self, path: str, payload: dict[str, Any]) -> Any:
        return self.request("POST", path, payload)

    def put(self, path: str, payload: dict[str, Any]) -> Any:
        return self.request("PUT", path, payload)


@dataclass
class CardSpec:
    key: str
    name: str
    display: str
    tab: str
    row: int
    col: int
    size_x: int
    size_y: int
    sql: str
    template_tags: dict[str, Any] | None = None
    visualization_settings: dict[str, Any] | None = None
    description: str | None = None
    inline_parameter_names: list[str] | None = None


def field_filter_tag(
    *,
    name: str,
    field_id: int,
    alias: str,
    widget_type: str,
    display_name: str,
    base_type: str = "type/Text",
    default: Any | None = None,
    required: bool = False,
) -> dict[str, Any]:
    tag = {
        "name": name,
        "display-name": display_name,
        "type": "dimension",
        "widget-type": widget_type,
        "dimension": ["field", {"base-type": base_type}, field_id],
        "alias": alias,
    }
    if default is not None:
        tag["default"] = default
    if required:
        tag["required"] = True
    return tag


def text_tag(*, name: str, display_name: str, default: Any | None = None, required: bool = False) -> dict[str, Any]:
    tag = {
        "name": name,
        "display-name": display_name,
        "type": "text",
    }
    if default is not None:
        tag["default"] = default
    if required:
        tag["required"] = True
    return tag


def stage_with_tags(sql: str, template_tags: dict[str, Any] | None = None) -> dict[str, Any]:
    stage = {
        "lib/type": "mbql.stage/native",
        "native": sql,
    }
    if template_tags:
        stage["template-tags"] = template_tags
    return stage


ORDERS_DATE_TAG = field_filter_tag(
    name="order_date",
    field_id=518,
    alias="mo.order_date",
    widget_type="date/all-options",
    display_name="Order Date",
    base_type="type/DateTime",
)
ORDERS_STATUS_TAG = field_filter_tag(
    name="status",
    field_id=519,
    alias="mo.status",
    widget_type="string/!=",
    display_name="Status",
)
ITEMS_DATE_TAG = field_filter_tag(
    name="order_date",
    field_id=508,
    alias="moi.order_date",
    widget_type="date/all-options",
    display_name="Order Date",
    base_type="type/DateTime",
)
ITEMS_STATUS_TAG = field_filter_tag(
    name="status",
    field_id=509,
    alias="moi.status",
    widget_type="string/!=",
    display_name="Status",
)
FINANCE_DATE_TAG = field_filter_tag(
    name="happened_at",
    field_id=501,
    alias="mfi.happened_at",
    widget_type="date/all-options",
    display_name="Happened At",
    base_type="type/DateTime",
)
ADS_DATE_TAG = field_filter_tag(
    name="stat_date",
    field_id=480,
    alias="mad.stat_date",
    widget_type="date/all-options",
    display_name="Stat Date",
    base_type="type/Date",
)
MARKETPLACE_ORDER_TAG = field_filter_tag(
    name="marketplace_name",
    field_id=516,
    alias="mo.marketplace",
    widget_type="string/=",
    display_name="Marketplace",
)
MARKETPLACE_ITEM_TAG = field_filter_tag(
    name="items_marketplace",
    field_id=506,
    alias="moi.marketplace",
    widget_type="string/=",
    display_name="Marketplace",
)
MARKETPLACE_FINANCE_TAG = field_filter_tag(
    name="finance_marketplace",
    field_id=499,
    alias="mfi.marketplace",
    widget_type="string/=",
    display_name="Marketplace",
)
MARKETPLACE_ADS_TAG = field_filter_tag(
    name="ads_marketplace",
    field_id=479,
    alias="mad.marketplace",
    widget_type="string/=",
    display_name="Marketplace",
)
MARKETPLACE_STOCK_TAG = field_filter_tag(
    name="stock_marketplace",
    field_id=537,
    alias="ms.marketplace",
    widget_type="string/=",
    display_name="Marketplace",
)
ORDER_ID_SEARCH_TAG = text_tag(name="order_id_search", display_name="Order ID Search")
GRANULARITY_TAG = text_tag(name="granularity", display_name="Granularity", default=["week"], required=True)
FLAVOR_TAG = field_filter_tag(
    name="flavor",
    field_id=140,
    alias="p.flavor",
    widget_type="string/=",
    display_name="Flavor",
)
GRAMS_TAG = field_filter_tag(
    name="grams",
    field_id=141,
    alias="p.grams",
    widget_type="number/=",
    display_name="Grams",
    base_type="type/Integer",
)
DAYS_BACK_TAG = text_tag(name="days_back", display_name="Days Back", default="30", required=True)
WAREHOUSE_INVENTORY_TAG = field_filter_tag(
    name="warehouse_name",
    field_id=241,
    alias="o.warehouse_name",
    widget_type="string/=",
    display_name="Warehouse Name",
)
MONTH_INVENTORY_TAG = field_filter_tag(
    name="month",
    field_id=111,
    alias="o.order_date",
    widget_type="date/month-year",
    display_name="Month",
    required=True,
    default="2025-10",
    base_type="type/DateTime",
)
ITEMS_WAREHOUSE_TAG = field_filter_tag(
    name="warehouse_name",
    field_id=514,
    alias="moi.warehouse_name",
    widget_type="string/=",
    display_name="Warehouse Name",
)
ITEMS_MONTH_TAG = field_filter_tag(
    name="month",
    field_id=508,
    alias="moi.order_date",
    widget_type="date/month-year",
    display_name="Month",
    required=True,
    default="2025-10",
    base_type="type/DateTime",
)
GENERAL_FLAVOR_TAG = field_filter_tag(
    name="flavor",
    field_id=570,
    alias="moi.flavor",
    widget_type="string/=",
    display_name="Flavor",
)
GENERAL_GRAMS_TAG = field_filter_tag(
    name="grams",
    field_id=571,
    alias="moi.grams",
    widget_type="number/=",
    display_name="Grams",
    base_type="type/Integer",
)
STOCK_WAREHOUSE_TAG = field_filter_tag(
    name="warehouse_name",
    field_id=539,
    alias="sc.warehouse_name",
    widget_type="string/=",
    display_name="Warehouse Name",
)


def scalar_currency_sql(where_clause: str = "", extra_filters: str = "") -> str:
    return f"""
SELECT
  COALESCE(SUM(revenue), 0) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
{where_clause}
{extra_filters};
""".strip()


def scalar_orders_sql(where_clause: str = "", extra_filters: str = "") -> str:
    return f"""
SELECT
  COUNT(DISTINCT order_id) AS orders_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
{where_clause}
{extra_filters};
""".strip()


def scalar_units_sql(where_clause: str = "", extra_from: str = "", extra_filters: str = "") -> str:
    return f"""
SELECT
  COALESCE(SUM(quantity), 0) AS units_total
FROM public.marketplace_order_items moi
{extra_from}
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
{where_clause}
{extra_filters};
""".strip()


def scalar_avg_sql(where_clause: str = "", extra_filters: str = "") -> str:
    return f"""
SELECT
  ROUND(COALESCE(AVG(revenue), 0), 2) AS average_order_value
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
{where_clause}
{extra_filters};
""".strip()


def build_generated_description(spec: CardSpec) -> str:
    return append_description_marker(spec.description, f"{GENERATED_CARD_MARKER}{spec.key}]")


def extract_generated_card_key(description: str | None) -> str | None:
    if not description:
        return None
    marker_start = description.rfind(GENERATED_CARD_MARKER)
    if marker_start == -1:
        return None
    marker_end = description.find("]", marker_start)
    if marker_end == -1:
        return None
    return description[marker_start + len(GENERATED_CARD_MARKER):marker_end] or None


def currency_scalar_settings(column_name: str) -> dict[str, Any]:
    return {
        "column_settings": {
            json.dumps(["name", column_name], ensure_ascii=False): {
                "number_separators": ", ",
                "suffix": " ₽",
            }
        }
    }


MARKETPLACE_DISPLAY_LABELS = {
    "ozon": "Ozon",
    "yandex_market": "Яндекс Маркет",
    "wildberries": "Wildberries",
}

MARKETPLACE_COLORS = {
    "Ozon": "#509EE3",
    "Яндекс Маркет": "#F9D45C",
    "Wildberries": "#A989C5",
}


def marketplace_label_sql(column_name: str = "marketplace") -> str:
    return f"""
CASE
  WHEN {column_name} = 'ozon' THEN 'Ozon'
  WHEN {column_name} = 'yandex_market' THEN 'Яндекс Маркет'
  WHEN {column_name} = 'wildberries' THEN 'Wildberries'
  ELSE {column_name}
END
""".strip()


def marketplace_series_visuals(*, metric_name: str, x_title: str, y_title: str, display: str = "line", stacked: str | None = None) -> dict[str, Any]:
    visuals: dict[str, Any] = {
        "graph.dimensions": ["period", "marketplace"],
        "graph.metrics": [metric_name],
        "graph.series_order": [
            {"color": MARKETPLACE_COLORS["Ozon"], "enabled": True, "key": "Ozon", "name": "Ozon"},
            {"color": MARKETPLACE_COLORS["Яндекс Маркет"], "enabled": True, "key": "Яндекс Маркет", "name": "Яндекс Маркет"},
            {"color": MARKETPLACE_COLORS["Wildberries"], "enabled": True, "key": "Wildberries", "name": "Wildberries"},
        ],
        "graph.series_order_dimension": "marketplace",
        "graph.x_axis.scale": "timeseries",
        "graph.x_axis.title_text": x_title,
        "graph.y_axis.title_text": y_title,
        "series_settings": {
            label: {"color": color, "display": display}
            for label, color in MARKETPLACE_COLORS.items()
        },
    }
    if stacked:
        visuals["stackable.stack_type"] = stacked
    return visuals


def marketplace_table_formatting(column_name: str = "marketplace") -> list[dict[str, Any]]:
    return [
        {
            "color": MARKETPLACE_COLORS["Ozon"],
            "columns": [column_name],
            "highlight_row": False,
            "operator": "=",
            "type": "single",
            "value": "Ozon",
        },
        {
            "color": MARKETPLACE_COLORS["Яндекс Маркет"],
            "columns": [column_name],
            "highlight_row": False,
            "operator": "=",
            "type": "single",
            "value": "Яндекс Маркет",
        },
        {
            "color": MARKETPLACE_COLORS["Wildberries"],
            "columns": [column_name],
            "highlight_row": False,
            "operator": "=",
            "type": "single",
            "value": "Wildberries",
        },
    ]


STATUS_TIMESERIES_VISUALS = {
    "graph.dimensions": ["period", "status"],
    "graph.metrics": ["orders_cnt"],
    "graph.series_order": [
        {"color": "hsla(358, 71%, 62%, 1)", "enabled": True, "key": "cancelled", "name": "cancelled"},
        {"color": "#227FD2", "enabled": True, "key": "delivering", "name": "delivering"},
        {"color": "#98D9D9", "enabled": True, "key": "awaiting_deliver", "name": "awaiting_deliver"},
        {"color": "#509EE3", "enabled": True, "key": "awaiting_packaging", "name": "awaiting_packaging"},
        {"color": "#88BF4D", "enabled": True, "key": "delivered", "name": "delivered"},
    ],
    "graph.series_order_dimension": "status",
    "graph.x_axis.scale": "timeseries",
    "graph.x_axis.title_text": "Дата",
    "graph.y_axis.auto_range": True,
    "graph.y_axis.auto_split": True,
    "graph.y_axis.title_text": "Колич",
    "series_settings": {
        "delivered": {"color": "#88BF4D"},
        "delivering": {"color": "#227FD2"},
    },
    "stackable.stack_type": "stacked",
}

PROFIT_COMMISSION_VISUALS = {
    "graph.dimensions": ["period"],
    "graph.metrics": ["revenue", "profit_before_all_fees", "order_commissions"],
    "graph.x_axis.scale": "timeseries",
    "graph.x_axis.title_text": "Неделя",
    "graph.y_axis.title_text": "Сумма, ₽",
    "series_settings": {
        "order_commissions": {"color": "#EF8C8C", "display": "area", "title": "Комиссии"},
        "profit_before_all_fees": {"color": "#88BF4D", "display": "area", "title": "Прибыль"},
        "revenue": {"color": "#509EE3", "line.marker_enabled": None, "title": "Выручка"},
    },
    "stackable.stack_type": "stacked",
}

COMMISSION_SHARE_VISUALS = {
    "graph.dimensions": ["period", "metric"],
    "graph.metrics": ["amount"],
    "graph.series_order": [
        {"color": "#F2F2F3", "enabled": True, "key": "Прочие комиссии", "name": "Прочие комиссии"},
        {"color": "#F2A86F", "enabled": True, "key": "Реклама (по заказам)", "name": "Реклама (по заказам)"},
        {"color": "#A989C5", "enabled": True, "key": "Эквайринг", "name": "Эквайринг"},
        {"color": "#509EE3", "enabled": True, "key": "Доставка", "name": "Доставка"},
        {"color": "#F9D45C", "enabled": True, "key": "Скидки", "name": "Скидки"},
        {"color": "#E75454", "enabled": True, "key": "Комиссия площадки", "name": "Комиссия площадки"},
        {"color": "#88BF4D", "enabled": True, "key": "Прибыль", "name": "Прибыль"},
    ],
    "graph.series_order_dimension": "metric",
    "graph.x_axis.scale": "timeseries",
    "graph.x_axis.title_text": "Неделя",
    "graph.y_axis.title_text": "Доля продаж",
    "series_settings": {
        "Доставка": {"color": "#509EE3"},
        "Комиссия площадки": {"color": "#E75454"},
        "Прибыль": {"color": "#88BF4D"},
        "Прочие комиссии": {"color": "#F2F2F3"},
        "Реклама (по заказам)": {"color": "#F2A86F"},
        "Скидки": {"color": "#F9D45C"},
        "Эквайринг": {"color": "#A989C5"},
    },
    "stackable.stack_type": "normalized",
}

STATUS_SPLIT_VISUALS = {
    "graph.dimensions": ["period"],
    "graph.metrics": ["first_orders", "repeat_orders"],
    "graph.x_axis.scale": "timeseries",
    "graph.x_axis.title_text": "Месяц",
    "graph.y_axis.title_text": "Количество заказов",
    "series_settings": {
        "first_orders": {"color": "#88BF4D", "title": "Доставленные"},
        "repeat_orders": {"color": "#F9D45C", "title": "Прочие"},
    },
}

STATUS_PIE_VISUALS = {
    "pie.dimension": ["status_label"],
    "pie.metric": "orders_cnt",
    "pie.sort_rows": False,
}

FEE_TABLE_VISUALS = {
    "column_settings": {
        json.dumps(["name", "promo_code"], ensure_ascii=False): {"column_title": "Тип расхода"},
        json.dumps(["name", "applied_count"], ensure_ascii=False): {"column_title": "Использования"},
        json.dumps(["name", "first_order_date_with_promo"], ensure_ascii=False): {"column_title": "Первая дата"},
        json.dumps(["name", "last_order_date_with_promo"], ensure_ascii=False): {"column_title": "Последняя дата"},
    },
    "table.cell_column": "first_order_date_with_promo",
    "table.columns": [
        {"enabled": True, "name": "promo_code"},
        {"enabled": True, "name": "applied_count"},
        {"enabled": True, "name": "first_order_date_with_promo"},
        {"enabled": True, "name": "last_order_date_with_promo"},
    ],
    "table.pivot": False,
    "table.pivot_column": "applied_count",
    "table.row_index": True,
}

WAREHOUSE_PIE_VISUALS = {
    "pie.dimension": ["cluster"],
    "pie.metric": "orders_cnt",
    "pie.sort_rows": False,
}

PERIODIC_COMMISSIONS_VISUALS = {
    "column_settings": {
        json.dumps(["name", "amount"], ensure_ascii=False): {"column_title": "Сумма"},
        json.dumps(["name", "fee_group"], ensure_ascii=False): {"column_title": "Группа расходов"},
        json.dumps(["name", "fee_name"], ensure_ascii=False): {"column_title": "Тип расходов"},
    },
    "table.cell_column": "amount",
    "table.pivot_column": "fee_group",
}

FEE_TIMESERIES_VISUALS = {
    "graph.dimensions": ["period", "fee_name"],
    "graph.metrics": ["amount"],
    "graph.series_order_dimension": "fee_name",
    "graph.x_axis.scale": "timeseries",
    "graph.x_axis.title_text": "Дата",
    "graph.y_axis.title_text": "Стоимость, ₽",
    "stackable.stack_type": "stacked",
}

MARKETPLACE_PIE_VISUALS = {
    "pie.dimension": ["marketplace"],
    "pie.metric": "orders_cnt",
    "pie.sort_rows": False,
    "pie.rows": [
        {"color": MARKETPLACE_COLORS["Ozon"], "enabled": True, "key": "Ozon", "name": "Ozon"},
        {"color": MARKETPLACE_COLORS["Яндекс Маркет"], "enabled": True, "key": "Яндекс Маркет", "name": "Яндекс Маркет"},
        {"color": MARKETPLACE_COLORS["Wildberries"], "enabled": True, "key": "Wildberries", "name": "Wildberries"},
    ],
}

GENERAL_COMMISSION_CATEGORIES_VISUALS = {
    "graph.dimensions": ["period", "metric"],
    "graph.metrics": ["amount"],
    "graph.series_order_dimension": "metric",
    "graph.x_axis.scale": "timeseries",
    "graph.x_axis.title_text": "Дата",
    "graph.y_axis.title_text": "Сумма, ₽",
    "stackable.stack_type": "stacked",
}


def general_orders_table_visuals() -> dict[str, Any]:
    return {
        "column_settings": {
            json.dumps(["name", "order_date"], ensure_ascii=False): {
                "column_title": "Дата",
                "date_separator": ".",
                "date_style": "D/M/YYYY",
                "time_enabled": "seconds",
                "time_style": "HH:mm",
            },
            json.dumps(["name", "marketplace"], ensure_ascii=False): {"column_title": "Площадка"},
            json.dumps(["name", "order_id"], ensure_ascii=False): {"column_title": "ID заказа"},
            json.dumps(["name", "status"], ensure_ascii=False): {"column_title": "Статус"},
            json.dumps(["name", "warehouse_name"], ensure_ascii=False): {"column_title": "Склад"},
            json.dumps(["name", "customer_key"], ensure_ascii=False): {"column_title": "ID покупателя"},
            json.dumps(["name", "revenue"], ensure_ascii=False): {"column_title": "Выручка"},
            json.dumps(["name", "commission_total"], ensure_ascii=False): {"column_title": "Комиссии"},
            json.dumps(["name", "profit_total"], ensure_ascii=False): {"column_title": "Прибыль"},
        },
        "table.cell_column": "revenue",
        "table.column_formatting": marketplace_table_formatting("marketplace") + [
            {
                "color": "#EF8C8C",
                "columns": ["status"],
                "highlight_row": False,
                "operator": "=",
                "type": "single",
                "value": "cancelled",
            },
            {
                "color": "#88BF4D",
                "columns": ["status"],
                "highlight_row": False,
                "operator": "=",
                "type": "single",
                "value": "delivered",
            },
        ],
        "table.columns": [
            {"enabled": True, "name": "order_date"},
            {"enabled": True, "name": "marketplace"},
            {"enabled": True, "name": "order_id"},
            {"enabled": True, "name": "status"},
            {"enabled": True, "name": "warehouse_name"},
            {"enabled": True, "name": "customer_key"},
            {"enabled": True, "name": "revenue"},
            {"enabled": True, "name": "commission_total"},
            {"enabled": True, "name": "profit_total"},
        ],
        "table.pivot": False,
    }


def general_periodic_commissions_visuals() -> dict[str, Any]:
    visuals = dict(PERIODIC_COMMISSIONS_VISUALS)
    visuals["column_settings"] = {
        **visuals["column_settings"],
        json.dumps(["name", "marketplace"], ensure_ascii=False): {"column_title": "Площадка"},
    }
    visuals["table.column_formatting"] = marketplace_table_formatting("marketplace")
    return visuals


def general_inventory_visuals() -> dict[str, Any]:
    return {
        "column_settings": {
            json.dumps(["name", "marketplace"], ensure_ascii=False): {"column_title": "Площадка"},
            json.dumps(["name", "warehouse_name"], ensure_ascii=False): {"column_title": "Склад"},
            json.dumps(["name", "sku"], ensure_ascii=False): {"column_title": "SKU"},
            json.dumps(["name", "product_name"], ensure_ascii=False): {"column_title": "Товар"},
            json.dumps(["name", "sold_units"], ensure_ascii=False): {"column_title": "Продано"},
            json.dumps(["name", "revenue_total"], ensure_ascii=False): {"column_title": "Выручка"},
        },
        "table.column_formatting": marketplace_table_formatting("marketplace"),
        "table.columns": [
            {"enabled": True, "name": "marketplace"},
            {"enabled": True, "name": "warehouse_name"},
            {"enabled": True, "name": "sku"},
            {"enabled": True, "name": "product_name"},
            {"enabled": True, "name": "sold_units"},
            {"enabled": True, "name": "revenue_total"},
        ],
    }


GENERAL_COMMON_TAGS = {
    "order_date": ORDERS_DATE_TAG,
    "status": ORDERS_STATUS_TAG,
}
GENERAL_SERIES_TAGS = {
    **GENERAL_COMMON_TAGS,
    "granularity": GRANULARITY_TAG,
}
ITEMS_COMMON_TAGS = {
    "order_date": ITEMS_DATE_TAG,
    "status": ITEMS_STATUS_TAG,
}
OZON_CATALOG_TAGS = {
    "flavor": FLAVOR_TAG,
    "grams": GRAMS_TAG,
}

PLATFORM_PROFIT_TAGS = {
    "order_date": ORDERS_DATE_TAG,
    "happened_at": FINANCE_DATE_TAG,
    "stat_date": ADS_DATE_TAG,
}
PLATFORM_SERIES_TAGS = {
    **PLATFORM_PROFIT_TAGS,
    "granularity": GRANULARITY_TAG,
    "status": ORDERS_STATUS_TAG,
}
PLATFORM_ORDER_TABLE_TAGS = {
    "order_date": ORDERS_DATE_TAG,
    "status": ORDERS_STATUS_TAG,
    "order_id_search": ORDER_ID_SEARCH_TAG,
}
PLATFORM_STOCK_TAGS = {
    "days_back": DAYS_BACK_TAG,
    "warehouse_name": STOCK_WAREHOUSE_TAG,
}
PLATFORM_MONTH_INVENTORY_TAGS = {
    "month": ITEMS_MONTH_TAG,
    "warehouse_name": ITEMS_WAREHOUSE_TAG,
}


def section_heading_dashcard(tab_id: int, text: str, dashcard_id: int, row: int) -> dict[str, Any]:
    return {
        "id": dashcard_id,
        "card_id": None,
        "row": row,
        "col": 0,
        "size_x": 24,
        "size_y": 1,
        "visualization_settings": {"virtual_card": {"display": "heading", "text": text}},
        "parameter_mappings": [],
        "series": [],
        "dashboard_tab_id": tab_id,
    }


def info_dashcard(tab_id: int, text: str, dashcard_id: int, row: int) -> dict[str, Any]:
    return {
        "id": dashcard_id,
        "card_id": None,
        "row": row,
        "col": 0,
        "size_x": 24,
        "size_y": 2,
        "visualization_settings": {"virtual_card": {"display": "text", "text": text}},
        "parameter_mappings": [],
        "series": [],
        "dashboard_tab_id": tab_id,
    }


def platform_total_customers_sql(marketplace: str) -> str:
    return f"""
SELECT
  COUNT(DISTINCT customer_key) AS customers_total
FROM public.marketplace_orders mo
WHERE mo.marketplace = '{marketplace}'
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
  AND customer_key IS NOT NULL
  AND TRIM(customer_key) <> '';
""".strip()


def platform_total_profit_sql(marketplace: str) -> str:
    return f"""
WITH revenue_agg AS (
  SELECT COALESCE(SUM(revenue), 0) AS revenue_total
  FROM public.marketplace_orders mo
  WHERE mo.marketplace = '{marketplace}'
    [[AND {{{{order_date}}}}]]
    AND mo.status = 'delivered'
),
finance_agg AS (
  SELECT COALESCE(SUM(amount), 0) AS commissions_total
  FROM public.marketplace_finance_items mfi
  WHERE mfi.marketplace = '{marketplace}'
    [[AND {{{{happened_at}}}}]]
),
ads_agg AS (
  SELECT COALESCE(SUM(spend), 0) AS ads_total
  FROM public.marketplace_ads_daily mad
  WHERE mad.marketplace = '{marketplace}'
    [[AND {{{{stat_date}}}}]]
)
SELECT
  revenue_total - commissions_total - ads_total AS profit_total
FROM revenue_agg
CROSS JOIN finance_agg
CROSS JOIN ads_agg;
""".strip()


def platform_units_by_dates_sql(marketplace: str) -> str:
    return f"""
SELECT
  date_trunc(
    CASE
      WHEN {{{{granularity}}}} = 'day' THEN 'day'
      WHEN {{{{granularity}}}} = 'week' THEN 'week'
      WHEN {{{{granularity}}}} = 'month' THEN 'month'
      WHEN {{{{granularity}}}} = 'year' THEN 'year'
      ELSE 'day'
    END,
    moi.order_date
  )::date AS period,
  COALESCE(moi.status, '(не указан)') AS status,
  COALESCE(SUM(moi.quantity), 0) AS orders_cnt
FROM public.marketplace_order_items moi
WHERE moi.marketplace = '{marketplace}'
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
GROUP BY 1, 2
ORDER BY 1, 2;
""".strip()


def platform_profit_and_commissions_sql(marketplace: str) -> str:
    return f"""
WITH orders_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{{{granularity}}}} = 'day' THEN 'day'
        WHEN {{{{granularity}}}} = 'week' THEN 'week'
        WHEN {{{{granularity}}}} = 'month' THEN 'month'
        WHEN {{{{granularity}}}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mo.order_date
    )::date AS period,
    SUM(mo.revenue) AS revenue
  FROM public.marketplace_orders mo
  WHERE mo.marketplace = '{marketplace}'
    [[AND {{{{order_date}}}}]]
    AND mo.status = 'delivered'
  GROUP BY 1
),
finance_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{{{granularity}}}} = 'day' THEN 'day'
        WHEN {{{{granularity}}}} = 'week' THEN 'week'
        WHEN {{{{granularity}}}} = 'month' THEN 'month'
        WHEN {{{{granularity}}}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mfi.happened_at
    )::date AS period,
    SUM(mfi.amount) AS order_commissions
  FROM public.marketplace_finance_items mfi
  WHERE mfi.marketplace = '{marketplace}'
    [[AND {{{{happened_at}}}}]]
  GROUP BY 1
),
periods AS (
  SELECT period FROM orders_agg
  UNION
  SELECT period FROM finance_agg
)
SELECT
  p.period,
  COALESCE(o.revenue, 0) AS revenue,
  COALESCE(o.revenue, 0) - COALESCE(f.order_commissions, 0) AS profit_before_all_fees,
  COALESCE(f.order_commissions, 0) AS order_commissions
FROM periods p
LEFT JOIN orders_agg o ON o.period = p.period
LEFT JOIN finance_agg f ON f.period = p.period
ORDER BY p.period;
""".strip()


def platform_commission_shares_sql(marketplace: str) -> str:
    return f"""
WITH revenue_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{{{granularity}}}} = 'day' THEN 'day'
        WHEN {{{{granularity}}}} = 'week' THEN 'week'
        WHEN {{{{granularity}}}} = 'month' THEN 'month'
        WHEN {{{{granularity}}}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mo.order_date
    )::date AS period,
    SUM(mo.revenue) AS revenue
  FROM public.marketplace_orders mo
  WHERE mo.marketplace = '{marketplace}'
    [[AND {{{{order_date}}}}]]
    AND mo.status = 'delivered'
  GROUP BY 1
),
finance_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{{{granularity}}}} = 'day' THEN 'day'
        WHEN {{{{granularity}}}} = 'week' THEN 'week'
        WHEN {{{{granularity}}}} = 'month' THEN 'month'
        WHEN {{{{granularity}}}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mfi.happened_at
    )::date AS period,
    SUM(mfi.amount) AS finance_total
  FROM public.marketplace_finance_items mfi
  WHERE mfi.marketplace = '{marketplace}'
    [[AND {{{{happened_at}}}}]]
  GROUP BY 1
),
ads_agg AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{{{granularity}}}} = 'day' THEN 'day'
        WHEN {{{{granularity}}}} = 'week' THEN 'week'
        WHEN {{{{granularity}}}} = 'month' THEN 'month'
        WHEN {{{{granularity}}}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mad.stat_date
    )::date AS period,
    SUM(mad.spend) AS ads_total
  FROM public.marketplace_ads_daily mad
  WHERE mad.marketplace = '{marketplace}'
    [[AND {{{{stat_date}}}}]]
  GROUP BY 1
),
periods AS (
  SELECT period FROM revenue_agg
  UNION
  SELECT period FROM finance_agg
  UNION
  SELECT period FROM ads_agg
),
base AS (
  SELECT
    p.period,
    COALESCE(r.revenue, 0) AS revenue,
    COALESCE(f.finance_total, 0) AS finance_total,
    COALESCE(a.ads_total, 0) AS ads_total
  FROM periods p
  LEFT JOIN revenue_agg r ON r.period = p.period
  LEFT JOIN finance_agg f ON f.period = p.period
  LEFT JOIN ads_agg a ON a.period = p.period
)
SELECT period, 'Комиссия площадки' AS metric, finance_total AS amount FROM base
UNION ALL
SELECT period, 'Реклама (по заказам)' AS metric, ads_total AS amount FROM base
UNION ALL
SELECT period, 'Прибыль' AS metric, revenue - finance_total - ads_total AS amount FROM base
ORDER BY period, metric;
""".strip()


def platform_status_split_sql(marketplace: str) -> str:
    return f"""
SELECT
  date_trunc(
    CASE
      WHEN {{{{granularity}}}} = 'day' THEN 'day'
      WHEN {{{{granularity}}}} = 'week' THEN 'week'
      WHEN {{{{granularity}}}} = 'month' THEN 'month'
      WHEN {{{{granularity}}}} = 'year' THEN 'year'
      ELSE 'month'
    END,
    mo.order_date
  )::date AS period,
  COUNT(DISTINCT mo.order_id) FILTER (WHERE mo.status = 'delivered') AS first_orders,
  COUNT(DISTINCT mo.order_id) FILTER (WHERE mo.status IS DISTINCT FROM 'delivered') AS repeat_orders
FROM public.marketplace_orders mo
WHERE mo.marketplace = '{marketplace}'
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
GROUP BY 1
ORDER BY 1;
""".strip()


def platform_status_pie_sql(marketplace: str) -> str:
    return f"""
SELECT
  COALESCE(NULLIF(TRIM(status), ''), '(не указан)') AS status_label,
  COUNT(DISTINCT order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE mo.marketplace = '{marketplace}'
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
GROUP BY 1
ORDER BY orders_cnt DESC;
""".strip()


def platform_fee_stats_sql(marketplace: str) -> str:
    return f"""
SELECT
  COALESCE(NULLIF(TRIM(fee_name), ''), '(не указан)') AS promo_code,
  MIN(happened_at)::date AS first_order_date_with_promo,
  MAX(happened_at)::date AS last_order_date_with_promo,
  COUNT(*) AS applied_count
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = '{marketplace}'
  [[AND {{{{happened_at}}}}]]
GROUP BY 1
ORDER BY first_order_date_with_promo DESC NULLS LAST;
""".strip()


def platform_warehouse_distribution_sql(marketplace: str) -> str:
    return f"""
SELECT
  COALESCE(NULLIF(TRIM(warehouse_name), ''), '(не указан)') AS cluster,
  COUNT(DISTINCT order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE mo.marketplace = '{marketplace}'
  [[AND {{{{order_date}}}}]]
  AND mo.status = 'delivered'
GROUP BY 1
ORDER BY orders_cnt DESC;
""".strip()


def platform_orders_table_sql(marketplace: str) -> str:
    fulfillment_select = ""
    if marketplace == "ozon":
        fulfillment_select = "  UPPER(COALESCE(mo.fulfillment_type, 'unknown')) AS fulfillment_type,\n"
    return f"""
WITH finance_by_order AS (
  SELECT order_id, SUM(amount) AS commission_total
  FROM public.marketplace_finance_items
  WHERE marketplace = '{marketplace}'
  GROUP BY 1
)
SELECT
  mo.order_date,
  mo.order_id,
  mo.status,
  mo.warehouse_name,
  mo.customer_key,
{fulfillment_select}  mo.revenue,
  COALESCE(f.commission_total, 0) AS commission_total,
  mo.revenue - COALESCE(f.commission_total, 0) AS profit_total
FROM public.marketplace_orders mo
LEFT JOIN finance_by_order f ON f.order_id = mo.order_id
WHERE mo.marketplace = '{marketplace}'
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
  [[AND CAST(mo.order_id AS text) ILIKE CONCAT('%', {{{{order_id_search}}}}, '%')]]
ORDER BY mo.order_date DESC NULLS LAST
LIMIT 200;
""".strip()


def platform_fee_name_timeseries_sql(marketplace: str) -> str:
    return f"""
SELECT
  date_trunc(
    CASE
      WHEN {{{{granularity}}}} = 'day' THEN 'day'
      WHEN {{{{granularity}}}} = 'week' THEN 'week'
      WHEN {{{{granularity}}}} = 'month' THEN 'month'
      WHEN {{{{granularity}}}} = 'year' THEN 'year'
      ELSE 'week'
    END,
    mfi.happened_at
  )::date AS period,
  COALESCE(NULLIF(TRIM(mfi.fee_name), ''), 'Прочие комиссии') AS fee_name,
  ABS(SUM(mfi.amount)) AS amount
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = '{marketplace}'
  [[AND {{{{happened_at}}}}]]
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
""".strip()


def platform_periodic_commissions_sql(marketplace: str) -> str:
    return f"""
SELECT
  happened_at::date AS cost_date,
  COALESCE(NULLIF(TRIM(fee_group), ''), 'marketplace') AS fee_group,
  COALESCE(NULLIF(TRIM(fee_name), ''), '(не указан)') AS fee_name,
  -ABS(amount) AS amount
FROM public.marketplace_finance_items mfi
WHERE mfi.marketplace = '{marketplace}'
  [[AND {{{{happened_at}}}}]]
  AND amount <> 0
ORDER BY cost_date DESC, fee_group, fee_name;
""".strip()


def platform_stock_analysis_sql(marketplace: str) -> str:
    return f"""
WITH sales AS (
  SELECT
    moi.sku,
    MAX(moi.product_name) AS product_name,
    SUM(moi.quantity) AS sold_units
  FROM public.marketplace_order_items moi
  WHERE moi.marketplace = '{marketplace}'
    AND moi.order_date >= CURRENT_DATE - ({{{{days_back}}}}::int || ' days')::interval
  GROUP BY 1
),
stocks AS (
  SELECT
    sc.sku,
    sc.warehouse_name,
    SUM(sc.quantity_available) AS quantity_available,
    SUM(COALESCE(sc.quantity_reserved, 0)) AS quantity_reserved,
    SUM(COALESCE(sc.quantity_in_transit, 0)) AS quantity_in_transit
  FROM public.marketplace_stocks_current sc
  WHERE sc.marketplace = '{marketplace}'
    [[AND {{{{warehouse_name}}}}]]
  GROUP BY 1, 2
)
SELECT
  stocks.sku AS sku,
  COALESCE(sales.product_name, '(без названия)') AS product_name,
  stocks.warehouse_name,
  COALESCE(stocks.quantity_available, 0) AS quantity_available,
  COALESCE(stocks.quantity_reserved, 0) AS quantity_reserved,
  COALESCE(stocks.quantity_in_transit, 0) AS quantity_in_transit,
  COALESCE(sales.sold_units, 0) AS sold_units_last_period
FROM stocks
LEFT JOIN sales ON sales.sku = stocks.sku
ORDER BY sold_units_last_period DESC, quantity_available DESC
LIMIT 200;
""".strip()


def platform_month_inventory_sales_sql(marketplace: str) -> str:
    return f"""
SELECT
  moi.warehouse_name,
  moi.sku,
  MAX(moi.product_name) AS product_name,
  SUM(moi.quantity) AS sold_units,
  SUM(moi.item_revenue) AS revenue_total
FROM public.marketplace_order_items moi
WHERE moi.marketplace = '{marketplace}'
  [[AND {{{{month}}}}]]
  [[AND {{{{warehouse_name}}}}]]
GROUP BY 1, 2
ORDER BY sold_units DESC, revenue_total DESC
LIMIT 200;
""".strip()


GENERAL_ORDER_TAGS = {
    "order_date": ORDERS_DATE_TAG,
    "status": ORDERS_STATUS_TAG,
    "marketplace_name": MARKETPLACE_ORDER_TAG,
    "flavor": GENERAL_FLAVOR_TAG,
    "grams": GENERAL_GRAMS_TAG,
}
GENERAL_ITEMS_TAGS = {
    "order_date": ITEMS_DATE_TAG,
    "status": ITEMS_STATUS_TAG,
    "items_marketplace": MARKETPLACE_ITEM_TAG,
    "flavor": GENERAL_FLAVOR_TAG,
    "grams": GENERAL_GRAMS_TAG,
}
GENERAL_FINANCE_TAGS = {
    "happened_at": FINANCE_DATE_TAG,
    "finance_marketplace": MARKETPLACE_FINANCE_TAG,
    "flavor": GENERAL_FLAVOR_TAG,
    "grams": GENERAL_GRAMS_TAG,
}
GENERAL_ADS_TAGS = {
    "stat_date": ADS_DATE_TAG,
    "ads_marketplace": MARKETPLACE_ADS_TAG,
}
GENERAL_PROFIT_TAGS = {
    **GENERAL_ORDER_TAGS,
    **GENERAL_FINANCE_TAGS,
    **GENERAL_ADS_TAGS,
}
GENERAL_PROFIT_SERIES_TAGS = {
    **GENERAL_PROFIT_TAGS,
    "granularity": GRANULARITY_TAG,
}
GENERAL_ITEMS_SERIES_TAGS = {
    **GENERAL_ITEMS_TAGS,
    "granularity": GRANULARITY_TAG,
}
GENERAL_ORDER_TABLE_TAGS = {
    **GENERAL_ORDER_TAGS,
    "order_id_search": ORDER_ID_SEARCH_TAG,
}
GENERAL_INVENTORY_TAGS = {
    "month": ITEMS_MONTH_TAG,
    "warehouse_name": ITEMS_WAREHOUSE_TAG,
    "items_marketplace": MARKETPLACE_ITEM_TAG,
    "flavor": GENERAL_FLAVOR_TAG,
    "grams": GENERAL_GRAMS_TAG,
}


def general_total_profit_sql() -> str:
    return """
WITH base_orders AS (
  SELECT
    mo.marketplace,
    mo.order_id,
    mo.revenue,
    o.ozon_payout,
    o.ozon_fees_total
  FROM public.marketplace_orders mo
  LEFT JOIN public.orders o
    ON mo.marketplace = 'ozon'
   AND o.order_id = mo.order_id
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{status}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
),
finance_agg AS (
  SELECT
    mfi.marketplace,
    mfi.order_id,
    COALESCE(SUM(mfi.amount), 0) AS commissions_total
  FROM public.marketplace_finance_items mfi
  GROUP BY 1, 2
)
SELECT COALESCE(
  SUM(
    CASE
      WHEN b.marketplace = 'ozon' THEN COALESCE(b.ozon_payout, b.revenue)
      ELSE b.revenue - COALESCE(f.commissions_total, 0)
    END
  ),
  0
) AS profit_total
FROM base_orders b
LEFT JOIN finance_agg f
  ON f.marketplace = b.marketplace
 AND f.order_id = b.order_id;
""".strip()


def general_delivered_orders_sql() -> str:
    return """
SELECT COUNT(DISTINCT order_id) AS delivered_orders_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{marketplace_name}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{grams}}
  )]]
  AND mo.status = 'delivered';
""".strip()


def general_total_sold_sql() -> str:
    return """
SELECT COUNT(DISTINCT mo.order_id) AS units_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{marketplace_name}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{grams}}
  )]];
""".strip()


def general_total_group_orders_sql() -> str:
    return """
WITH base_orders AS (
  SELECT
    mo.marketplace,
    mo.order_id,
    CASE
      WHEN mo.marketplace = 'ozon' THEN COALESCE(o.order_group_id, regexp_replace(mo.order_id::text, '-\\d+$', ''))
      ELSE regexp_replace(mo.order_id::text, '[-_]\\d+$', '')
    END AS group_key
  FROM public.marketplace_orders mo
  LEFT JOIN public.orders o
    ON mo.marketplace = 'ozon'
   AND o.order_id = mo.order_id
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{status}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
)
SELECT COUNT(DISTINCT group_key) AS orders_total
FROM base_orders;
""".strip()


def general_total_customers_sql() -> str:
    return """
SELECT COUNT(DISTINCT customer_key) AS customers_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{marketplace_name}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{grams}}
  )]]
  AND customer_key IS NOT NULL
  AND TRIM(customer_key) <> '';
""".strip()


def general_units_by_dates_sql() -> str:
    return """
SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day' THEN 'day'
      WHEN {{granularity}} = 'week' THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year' THEN 'year'
      ELSE 'day'
    END,
    moi.order_date
  )::date AS period,
  COALESCE(moi.status, '(не указан)') AS status,
  COALESCE(SUM(moi.quantity), 0) AS orders_cnt
FROM public.marketplace_order_items_enriched moi
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{items_marketplace}}]]
  [[AND {{flavor}}]]
  [[AND {{grams}}]]
GROUP BY 1, 2
ORDER BY 1, 2;
""".strip()


def general_profit_commissions_sql() -> str:
    return """
WITH base_orders AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day' THEN 'day'
        WHEN {{granularity}} = 'week' THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mo.order_date
    )::date AS period,
    mo.marketplace,
    mo.order_id,
    mo.revenue,
    o.ozon_payout,
    o.ozon_fees_total
  FROM public.marketplace_orders mo
  LEFT JOIN public.orders o
    ON mo.marketplace = 'ozon'
   AND o.order_id = mo.order_id
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
    AND mo.status = 'delivered'
),
order_finance AS (
  SELECT
    b.marketplace,
    b.order_id,
    CASE
      WHEN b.marketplace = 'ozon' THEN ABS(COALESCE(MAX(b.ozon_fees_total), 0))
      ELSE COALESCE(SUM(mfi.amount), 0)
    END AS order_commissions
  FROM base_orders b
  LEFT JOIN public.marketplace_finance_items mfi
    ON mfi.marketplace = b.marketplace
   AND mfi.order_id = b.order_id
  GROUP BY 1, 2
),
period_agg AS (
  SELECT
    b.period,
    SUM(b.revenue) AS revenue,
    SUM(
      CASE
        WHEN b.marketplace = 'ozon' THEN b.revenue - ABS(COALESCE(b.ozon_fees_total, 0))
        ELSE b.revenue - COALESCE(f.order_commissions, 0)
      END
    ) AS profit_before_all_fees,
    SUM(
      CASE
        WHEN b.marketplace = 'ozon' THEN ABS(COALESCE(b.ozon_fees_total, 0))
        ELSE COALESCE(f.order_commissions, 0)
      END
    ) AS order_commissions
  FROM base_orders b
  LEFT JOIN order_finance f
    ON f.marketplace = b.marketplace
   AND f.order_id = b.order_id
  GROUP BY 1
),
bounds AS (
  SELECT MIN(period) AS min_period, MAX(period) AS max_period
  FROM period_agg
),
periods AS (
  SELECT
    generate_series(
      min_period,
      max_period,
      CASE
        WHEN {{granularity}} = 'day'   THEN interval '1 day'
        WHEN {{granularity}} = 'week'  THEN interval '1 week'
        WHEN {{granularity}} = 'month' THEN interval '1 month'
        WHEN {{granularity}} = 'year'  THEN interval '1 year'
        ELSE interval '1 week'
      END
    )::date AS period
  FROM bounds
  WHERE min_period IS NOT NULL AND max_period IS NOT NULL
),
series AS (
  SELECT
    p.period,
    COALESCE(a.revenue, 0) AS revenue,
    COALESCE(a.profit_before_all_fees, 0) AS profit_before_all_fees,
    COALESCE(a.order_commissions, 0) AS order_commissions
  FROM periods p
  LEFT JOIN period_agg a ON a.period = p.period
),
trim AS (
  SELECT
    MAX(period) FILTER (
      WHERE revenue <> 0 OR profit_before_all_fees <> 0 OR order_commissions <> 0
    ) AS last_nonzero_period
  FROM series
)
SELECT
  s.period,
  s.revenue,
  s.profit_before_all_fees,
  s.order_commissions
FROM series s
CROSS JOIN trim t
WHERE t.last_nonzero_period IS NULL OR s.period <= t.last_nonzero_period
ORDER BY s.period;
""".strip()


def general_successful_orders_by_marketplace_sql() -> str:
    return f"""
SELECT
  date_trunc(
    CASE
      WHEN {{{{granularity}}}} = 'day' THEN 'day'
      WHEN {{{{granularity}}}} = 'week' THEN 'week'
      WHEN {{{{granularity}}}} = 'month' THEN 'month'
      WHEN {{{{granularity}}}} = 'year' THEN 'year'
      ELSE 'day'
    END,
    mo.order_date
  )::date AS period,
  {marketplace_label_sql("mo.marketplace")} AS marketplace,
  COUNT(DISTINCT mo.order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{marketplace_name}}}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{flavor}}}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{grams}}}}
  )]]
  AND mo.status = 'delivered'
GROUP BY 1, 2
ORDER BY 1, 2;
""".strip()


def general_revenue_by_marketplace_sql() -> str:
    return f"""
SELECT
  date_trunc(
    CASE
      WHEN {{{{granularity}}}} = 'day' THEN 'day'
      WHEN {{{{granularity}}}} = 'week' THEN 'week'
      WHEN {{{{granularity}}}} = 'month' THEN 'month'
      WHEN {{{{granularity}}}} = 'year' THEN 'year'
      ELSE 'day'
    END,
    mo.order_date
  )::date AS period,
  {marketplace_label_sql("mo.marketplace")} AS marketplace,
  SUM(mo.revenue) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{marketplace_name}}}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{flavor}}}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{grams}}}}
  )]]
  AND mo.status = 'delivered'
GROUP BY 1, 2
ORDER BY 1, 2;
""".strip()


def general_profit_by_marketplace_sql() -> str:
    return f"""
WITH base_orders AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{{{granularity}}}} = 'day' THEN 'day'
        WHEN {{{{granularity}}}} = 'week' THEN 'week'
        WHEN {{{{granularity}}}} = 'month' THEN 'month'
        WHEN {{{{granularity}}}} = 'year' THEN 'year'
        ELSE 'day'
      END,
      mo.order_date
    )::date AS period,
    mo.marketplace,
    mo.order_id,
    mo.revenue,
    o.ozon_payout,
    o.ozon_fees_total
  FROM public.marketplace_orders mo
  LEFT JOIN public.orders o
    ON mo.marketplace = 'ozon'
   AND o.order_id = mo.order_id
  WHERE 1=1
    [[AND {{{{order_date}}}}]]
    [[AND {{{{marketplace_name}}}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{{{flavor}}}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{{{grams}}}}
    )]]
    AND mo.status = 'delivered'
),
order_finance AS (
  SELECT
    b.marketplace,
    b.order_id,
    COALESCE(SUM(mfi.amount), 0) AS order_commissions
  FROM base_orders b
  LEFT JOIN public.marketplace_finance_items mfi
    ON mfi.marketplace = b.marketplace
   AND mfi.order_id = b.order_id
  GROUP BY 1, 2
),
period_marketplace_agg AS (
  SELECT
    b.period,
    b.marketplace,
    SUM(
      CASE
        WHEN b.marketplace = 'ozon' THEN b.revenue - ABS(COALESCE(b.ozon_fees_total, 0))
        ELSE b.revenue - COALESCE(f.order_commissions, 0)
      END
    ) AS profit_total
  FROM base_orders b
  LEFT JOIN order_finance f
    ON f.marketplace = b.marketplace
   AND f.order_id = b.order_id
  GROUP BY 1, 2
)
SELECT
  period,
  {marketplace_label_sql("marketplace")} AS marketplace,
  profit_total
FROM period_marketplace_agg
ORDER BY 1, 2;
""".strip()


def general_repeat_orders_histogram_sql() -> str:
    return """
WITH base AS (
  SELECT
    mo.marketplace,
    mo.order_id,
    mo.order_date,
    mo.status,
    regexp_replace(mo.order_id::text, '-\\d+$', '') AS group_key,
    CASE
      WHEN mo.marketplace = 'ozon' AND mo.customer_key IS NOT NULL AND TRIM(mo.customer_key) <> '' THEN mo.customer_key
      WHEN mo.order_id LIKE '%-%' THEN split_part(regexp_replace(mo.order_id::text, '-\\d+$', ''), '-', 1)
      WHEN mo.order_id LIKE '%_%' THEN split_part(regexp_replace(mo.order_id::text, '_\\d+$', ''), '_', 1)
      ELSE COALESCE(NULLIF(TRIM(mo.customer_key), ''), mo.order_id)
    END AS customer_guess
  FROM public.marketplace_orders mo
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{status}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
),
groups AS (
  SELECT
    customer_guess,
    group_key,
    MIN(order_date) AS group_first_date,
    BOOL_OR(status = 'delivered') AS has_delivered
  FROM base
  WHERE status <> 'cancelled'
  GROUP BY 1, 2
),
delivered_groups AS (
  SELECT
    customer_guess,
    group_key,
    group_first_date,
    DENSE_RANK() OVER (
      PARTITION BY customer_guess
      ORDER BY group_first_date, group_key
    ) AS delivered_rank
  FROM groups
  WHERE has_delivered
)
SELECT
  date_trunc(
    CASE
      WHEN {{granularity}} = 'day' THEN 'day'
      WHEN {{granularity}} = 'week' THEN 'week'
      WHEN {{granularity}} = 'month' THEN 'month'
      WHEN {{granularity}} = 'year' THEN 'year'
      ELSE 'month'
    END,
    group_first_date
  )::date AS period,
  COUNT(*) FILTER (WHERE delivered_rank = 1) AS first_orders,
  COUNT(*) FILTER (WHERE delivered_rank > 1) AS repeat_orders
FROM delivered_groups
GROUP BY 1
ORDER BY 1;
""".strip()


def general_repeat_orders_pie_sql() -> str:
    return """
WITH base AS (
  SELECT
    mo.order_id,
    mo.order_date,
    mo.status,
    regexp_replace(mo.order_id::text, '-\\d+$', '') AS group_key,
    CASE
      WHEN mo.marketplace = 'ozon' AND mo.customer_key IS NOT NULL AND TRIM(mo.customer_key) <> '' THEN mo.customer_key
      WHEN mo.order_id LIKE '%-%' THEN split_part(regexp_replace(mo.order_id::text, '-\\d+$', ''), '-', 1)
      WHEN mo.order_id LIKE '%_%' THEN split_part(regexp_replace(mo.order_id::text, '_\\d+$', ''), '_', 1)
      ELSE COALESCE(NULLIF(TRIM(mo.customer_key), ''), mo.order_id)
    END AS customer_guess
  FROM public.marketplace_orders mo
  WHERE 1=1
    [[AND {{order_date}}]]
    [[AND {{status}}]]
    [[AND {{marketplace_name}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mo.order_id
        AND moi.marketplace = mo.marketplace
        AND {{grams}}
    )]]
),
groups AS (
  SELECT
    customer_guess,
    group_key,
    MIN(order_date) AS group_first_date,
    BOOL_OR(status = 'delivered') AS has_delivered
  FROM base
  WHERE status <> 'cancelled'
  GROUP BY 1, 2
),
delivered_groups AS (
  SELECT
    customer_guess,
    DENSE_RANK() OVER (
      PARTITION BY customer_guess
      ORDER BY group_first_date, group_key
    ) AS delivered_rank
  FROM groups
  WHERE has_delivered
),
customers_max AS (
  SELECT customer_guess, MAX(delivered_rank) AS orders_count
  FROM delivered_groups
  GROUP BY 1
)
SELECT
  orders_count,
  COUNT(*) AS customers
FROM customers_max
GROUP BY 1
ORDER BY 1;
""".strip()


def general_marketplace_distribution_sql() -> str:
    return f"""
SELECT
  {marketplace_label_sql("mo.marketplace")} AS marketplace,
  COUNT(DISTINCT mo.order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{marketplace_name}}}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{flavor}}}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{grams}}}}
  )]]
  AND mo.status = 'delivered'
GROUP BY 1
ORDER BY orders_cnt DESC;
""".strip()


def general_warehouse_distribution_sql() -> str:
    return """
SELECT
  COALESCE(NULLIF(TRIM(mo.warehouse_name), ''), '(не указан)') AS cluster,
  COUNT(DISTINCT mo.order_id) AS orders_cnt
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{marketplace_name}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{grams}}
  )]]
  AND mo.status = 'delivered'
GROUP BY 1
ORDER BY orders_cnt DESC;
""".strip()


def general_orders_table_sql() -> str:
    return f"""
WITH finance_by_order AS (
  SELECT marketplace, order_id, SUM(amount) AS commission_total
  FROM public.marketplace_finance_items
  GROUP BY 1, 2
)
SELECT
  mo.order_date,
  {marketplace_label_sql("mo.marketplace")} AS marketplace,
  mo.order_id,
  mo.status,
  mo.warehouse_name,
  mo.customer_key,
  mo.revenue,
  COALESCE(f.commission_total, 0) AS commission_total,
  mo.revenue - COALESCE(f.commission_total, 0) AS profit_total
FROM public.marketplace_orders mo
LEFT JOIN finance_by_order f
  ON f.marketplace = mo.marketplace AND f.order_id = mo.order_id
WHERE 1=1
  [[AND {{{{order_date}}}}]]
  [[AND {{{{status}}}}]]
  [[AND {{{{marketplace_name}}}}]]
  [[AND CAST(mo.order_id AS text) ILIKE CONCAT('%', {{{{order_id_search}}}}, '%')]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{flavor}}}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{{{grams}}}}
  )]]
ORDER BY mo.order_date DESC NULLS LAST
LIMIT 200;
""".strip()


def general_commission_categories_sql() -> str:
    return """
WITH finance_rows AS (
  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day' THEN 'day'
        WHEN {{granularity}} = 'week' THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mfi.happened_at
    )::date AS period,
    COALESCE(NULLIF(mfi.fee_group, ''), NULLIF(mfi.fee_name, ''), 'Прочие комиссии') AS metric,
    ABS(SUM(mfi.amount)) AS amount
  FROM public.marketplace_finance_items mfi
  WHERE 1=1
    [[AND {{happened_at}}]]
    [[AND {{finance_marketplace}}]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mfi.order_id
        AND moi.marketplace = mfi.marketplace
        AND {{flavor}}
    )]]
    [[AND EXISTS (
      SELECT 1
      FROM public.marketplace_order_items_enriched moi
      WHERE moi.order_id = mfi.order_id
        AND moi.marketplace = mfi.marketplace
        AND {{grams}}
    )]]
  GROUP BY 1, 2

  UNION ALL

  SELECT
    date_trunc(
      CASE
        WHEN {{granularity}} = 'day' THEN 'day'
        WHEN {{granularity}} = 'week' THEN 'week'
        WHEN {{granularity}} = 'month' THEN 'month'
        WHEN {{granularity}} = 'year' THEN 'year'
        ELSE 'week'
      END,
      mad.stat_date
    )::date AS period,
    'Реклама (по заказам)' AS metric,
    ABS(SUM(mad.spend)) AS amount
  FROM public.marketplace_ads_daily mad
  WHERE 1=1
    [[AND {{stat_date}}]]
    [[AND {{ads_marketplace}}]]
  GROUP BY 1
)
SELECT period, metric, SUM(amount) AS amount
FROM finance_rows
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
""".strip()


def general_additional_services_sql() -> str:
    return f"""
SELECT
  date_trunc(
    CASE
      WHEN {{{{granularity}}}} = 'day' THEN 'day'
      WHEN {{{{granularity}}}} = 'week' THEN 'week'
      WHEN {{{{granularity}}}} = 'month' THEN 'month'
      WHEN {{{{granularity}}}} = 'year' THEN 'year'
      ELSE 'week'
    END,
    mfi.happened_at
  )::date AS period,
  {marketplace_label_sql("mfi.marketplace")} AS marketplace,
  ABS(SUM(mfi.amount)) AS amount
FROM public.marketplace_finance_items mfi
WHERE 1=1
  AND mfi.happened_at IS NOT NULL
  [[AND {{{{happened_at}}}}]]
  [[AND {{{{finance_marketplace}}}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mfi.order_id
      AND moi.marketplace = mfi.marketplace
      AND {{{{flavor}}}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mfi.order_id
      AND moi.marketplace = mfi.marketplace
      AND {{{{grams}}}}
  )]]
GROUP BY 1, 2
ORDER BY 1, 2;
""".strip()


def general_periodic_commissions_sql() -> str:
    return f"""
SELECT
  {marketplace_label_sql("mfi.marketplace")} AS marketplace,
  mfi.happened_at::date AS cost_date,
  COALESCE(NULLIF(TRIM(mfi.fee_group), ''), 'marketplace') AS fee_group,
  COALESCE(NULLIF(TRIM(mfi.fee_name), ''), '(не указан)') AS fee_name,
  -ABS(mfi.amount) AS amount
FROM public.marketplace_finance_items mfi
WHERE 1=1
  [[AND {{{{happened_at}}}}]]
  [[AND {{{{finance_marketplace}}}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mfi.order_id
      AND moi.marketplace = mfi.marketplace
      AND {{{{flavor}}}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mfi.order_id
      AND moi.marketplace = mfi.marketplace
      AND {{{{grams}}}}
  )]]
  AND mfi.amount <> 0
ORDER BY cost_date DESC, marketplace, fee_group, fee_name;
""".strip()


def general_inventory_sql() -> str:
    return f"""
SELECT
  {marketplace_label_sql("moi.marketplace")} AS marketplace,
  moi.warehouse_name,
  moi.sku,
  MAX(moi.product_name) AS product_name,
  SUM(moi.quantity) AS sold_units,
  SUM(moi.item_revenue) AS revenue_total
FROM public.marketplace_order_items_enriched moi
WHERE 1=1
  [[AND {{{{month}}}}]]
  [[AND {{{{warehouse_name}}}}]]
  [[AND {{{{items_marketplace}}}}]]
  [[AND {{{{flavor}}}}]]
  [[AND {{{{grams}}}}]]
GROUP BY 1, 2, 3
ORDER BY sold_units DESC, revenue_total DESC
LIMIT 200;
""".strip()


GENERAL_CARD_SPECS: list[CardSpec] = [
    CardSpec(
        key="general__01_total_units",
        name="Продано товаров",
        display="scalar",
        tab="Общее",
        row=1,
        col=0,
        size_x=4,
        size_y=3,
        sql=general_total_sold_sql(),
        template_tags=GENERAL_ORDER_TAGS,
    ),
    CardSpec(
        key="general__02_total_orders",
        name="Оформлено заказов",
        display="scalar",
        tab="Общее",
        row=1,
        col=4,
        size_x=4,
        size_y=3,
        sql=general_total_group_orders_sql(),
        template_tags=GENERAL_ORDER_TAGS,
    ),
    CardSpec(
        key="general__03_total_delivered",
        name="Всего покупателей",
        display="scalar",
        tab="Общее",
        row=1,
        col=8,
        size_x=4,
        size_y=3,
        sql=general_total_customers_sql(),
        template_tags={**GENERAL_ORDER_TAGS},
    ),
    CardSpec(
        key="general__04_total_revenue",
        name="Выручка",
        display="scalar",
        tab="Общее",
        row=1,
        col=12,
        size_x=4,
        size_y=3,
        sql="""
SELECT
  COALESCE(SUM(revenue), 0) AS revenue_total
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{marketplace_name}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{grams}}
  )]];
""".strip(),
        template_tags=GENERAL_ORDER_TAGS,
        visualization_settings=currency_scalar_settings("revenue_total"),
    ),
    CardSpec(
        key="general__05_total_profit",
        name="Прибыль",
        display="scalar",
        tab="Общее",
        row=1,
        col=16,
        size_x=4,
        size_y=3,
        sql=general_total_profit_sql(),
        template_tags=GENERAL_PROFIT_TAGS,
        visualization_settings=currency_scalar_settings("profit_total"),
    ),
    CardSpec(
        key="general__06_average_order_value",
        name="Средний чек",
        display="scalar",
        tab="Общее",
        row=1,
        col=20,
        size_x=4,
        size_y=3,
        sql="""
SELECT
  ROUND(COALESCE(AVG(revenue), 0), 2) AS average_order_value
FROM public.marketplace_orders mo
WHERE 1=1
  [[AND {{order_date}}]]
  [[AND {{status}}]]
  [[AND {{marketplace_name}}]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{flavor}}
  )]]
  [[AND EXISTS (
    SELECT 1
    FROM public.marketplace_order_items_enriched moi
    WHERE moi.order_id = mo.order_id
      AND moi.marketplace = mo.marketplace
      AND {{grams}}
  )]];
""".strip(),
        template_tags=GENERAL_ORDER_TAGS,
        visualization_settings=currency_scalar_settings("average_order_value"),
    ),
    CardSpec(
        key="general__07_units_by_dates",
        name="Количество купленных товаров по датам",
        display="bar",
        tab="Общее",
        row=4,
        col=0,
        size_x=12,
        size_y=6,
        sql=general_units_by_dates_sql(),
        template_tags=GENERAL_ITEMS_SERIES_TAGS,
        visualization_settings=STATUS_TIMESERIES_VISUALS,
    ),
    CardSpec(
        key="general__08_profit_commissions",
        name="Прибыль и комиссии по всем площадкам",
        display="combo",
        tab="Общее",
        row=4,
        col=12,
        size_x=12,
        size_y=6,
        sql=general_profit_commissions_sql(),
        template_tags=GENERAL_PROFIT_SERIES_TAGS,
        visualization_settings=PROFIT_COMMISSION_VISUALS,
    ),
    CardSpec(
        key="general__09_successful_orders_by_marketplace",
        name="Успешные заказы по площадкам",
        display="bar",
        tab="Общее",
        row=11,
        col=0,
        size_x=12,
        size_y=6,
        sql=general_successful_orders_by_marketplace_sql(),
        template_tags={**GENERAL_ORDER_TAGS, "granularity": GRANULARITY_TAG},
        visualization_settings=marketplace_series_visuals(metric_name="orders_cnt", x_title="Дата", y_title="Успешные заказы", display="bar", stacked="stacked"),
    ),
    CardSpec(
        key="general__10_revenue_by_marketplace",
        name="Выручка по площадкам по дням",
        display="line",
        tab="Общее",
        row=11,
        col=12,
        size_x=12,
        size_y=6,
        sql=general_revenue_by_marketplace_sql(),
        template_tags={**GENERAL_ORDER_TAGS, "granularity": GRANULARITY_TAG},
        visualization_settings=marketplace_series_visuals(metric_name="revenue_total", x_title="Дата", y_title="Выручка, ₽"),
    ),
    CardSpec(
        key="general__11_profit_by_marketplace",
        name="Прибыль по площадкам по дням",
        display="line",
        tab="Общее",
        row=18,
        col=0,
        size_x=24,
        size_y=6,
        sql=general_profit_by_marketplace_sql(),
        template_tags=GENERAL_PROFIT_SERIES_TAGS,
        visualization_settings=marketplace_series_visuals(metric_name="profit_total", x_title="Дата", y_title="Прибыль, ₽"),
    ),
    CardSpec(
        key="general__12_repeat_histogram",
        name="Первые и повторные заказы по месяцам",
        display="bar",
        tab="Общее",
        row=26,
        col=0,
        size_x=16,
        size_y=6,
        sql=general_repeat_orders_histogram_sql(),
        template_tags={**GENERAL_ORDER_TAGS, "granularity": GRANULARITY_TAG},
        visualization_settings={
            "graph.dimensions": ["period"],
            "graph.metrics": ["first_orders", "repeat_orders"],
            "graph.x_axis.scale": "timeseries",
            "graph.x_axis.title_text": "Месяц",
            "graph.y_axis.title_text": "Количество заказов",
            "series_settings": {
                "first_orders": {"color": "#88BF4D", "title": "Первые"},
                "repeat_orders": {"color": "#F9D45C", "title": "Повторные"},
            },
        },
    ),
    CardSpec(
        key="general__13_repeat_pie",
        name="Повторные заказы - кольцо",
        display="pie",
        tab="Общее",
        row=26,
        col=16,
        size_x=8,
        size_y=6,
        sql=general_repeat_orders_pie_sql(),
        template_tags=GENERAL_ORDER_TAGS,
        visualization_settings={
            "pie.dimension": ["orders_count"],
            "pie.metric": "customers",
            "pie.sort_rows": False,
        },
    ),
    CardSpec(
        key="general__14_marketplace_distribution",
        name="Распределение заказов по площадкам",
        display="pie",
        tab="Общее",
        row=34,
        col=0,
        size_x=8,
        size_y=6,
        sql=general_marketplace_distribution_sql(),
        template_tags=GENERAL_ORDER_TAGS,
        visualization_settings=MARKETPLACE_PIE_VISUALS,
    ),
    CardSpec(
        key="general__15_warehouse_distribution",
        name="Распределение доставленных заказов по складам",
        display="pie",
        tab="Общее",
        row=34,
        col=8,
        size_x=16,
        size_y=6,
        sql=general_warehouse_distribution_sql(),
        template_tags=GENERAL_ORDER_TAGS,
        visualization_settings=WAREHOUSE_PIE_VISUALS,
    ),
    CardSpec(
        key="general__16_orders_table",
        name="Таблица заказов",
        display="table",
        tab="Общее",
        row=42,
        col=0,
        size_x=24,
        size_y=9,
        sql=general_orders_table_sql(),
        template_tags=GENERAL_ORDER_TABLE_TAGS,
        visualization_settings=general_orders_table_visuals(),
        inline_parameter_names=["Поиск по ID"],
    ),
    CardSpec(
        key="general__17_commission_categories",
        name="Комиссии по категориям",
        display="area",
        tab="Общее",
        row=53,
        col=0,
        size_x=24,
        size_y=6,
        sql=general_commission_categories_sql(),
        template_tags={**GENERAL_FINANCE_TAGS, **GENERAL_ADS_TAGS, "granularity": GRANULARITY_TAG},
        visualization_settings=GENERAL_COMMISSION_CATEGORIES_VISUALS,
    ),
    CardSpec(
        key="general__18_additional_services",
        name="Дополнительные услуги",
        display="area",
        tab="Общее",
        row=61,
        col=0,
        size_x=24,
        size_y=6,
        sql=general_additional_services_sql(),
        template_tags={**GENERAL_FINANCE_TAGS, "granularity": GRANULARITY_TAG},
        visualization_settings=marketplace_series_visuals(metric_name="amount", x_title="Дата", y_title="Стоимость, ₽", display="area", stacked="stacked"),
    ),
    CardSpec(
        key="general__19_periodic_commissions",
        name="Периодические комиссии",
        display="table",
        tab="Общее",
        row=69,
        col=0,
        size_x=24,
        size_y=8,
        sql=general_periodic_commissions_sql(),
        template_tags=GENERAL_FINANCE_TAGS,
        visualization_settings=general_periodic_commissions_visuals(),
    ),
    CardSpec(
        key="general__20_inventory",
        name="Инвентаризация - продажи за месяц",
        display="table",
        tab="Общее",
        row=79,
        col=0,
        size_x=24,
        size_y=7,
        sql=general_inventory_sql(),
        template_tags=GENERAL_INVENTORY_TAGS,
        visualization_settings=general_inventory_visuals(),
        inline_parameter_names=["Месяц инв.", "Склад инв."],
    ),
]


def platform_card_specs(marketplace: str, label: str, tab: str) -> list[CardSpec]:
    order_where = f"  AND marketplace = '{marketplace}'"
    return [
        CardSpec(
            key=f"{marketplace}__01_total_units",
            name="Продано товаров",
            display="scalar",
            tab=tab,
            row=1,
            col=0,
            size_x=4,
            size_y=3,
            sql=scalar_units_sql(order_where),
            template_tags=ITEMS_COMMON_TAGS,
        ),
        CardSpec(
            key=f"{marketplace}__02_total_orders",
            name="Оформлено заказов",
            display="scalar",
            tab=tab,
            row=1,
            col=4,
            size_x=4,
            size_y=3,
            sql=scalar_orders_sql(order_where),
            template_tags=GENERAL_COMMON_TAGS,
        ),
        CardSpec(
            key=f"{marketplace}__03_total_customers",
            name="Всего покупателей",
            display="scalar",
            tab=tab,
            row=1,
            col=8,
            size_x=4,
            size_y=3,
            sql=platform_total_customers_sql(marketplace),
            template_tags=GENERAL_COMMON_TAGS,
        ),
        CardSpec(
            key=f"{marketplace}__04_total_revenue",
            name="Выручка",
            display="scalar",
            tab=tab,
            row=1,
            col=12,
            size_x=4,
            size_y=3,
            sql=scalar_currency_sql(order_where),
            template_tags=GENERAL_COMMON_TAGS,
            visualization_settings=currency_scalar_settings("revenue_total"),
        ),
        CardSpec(
            key=f"{marketplace}__05_total_profit",
            name="Прибыль",
            display="scalar",
            tab=tab,
            row=1,
            col=16,
            size_x=4,
            size_y=3,
            sql=platform_total_profit_sql(marketplace),
            template_tags=PLATFORM_PROFIT_TAGS,
            visualization_settings=currency_scalar_settings("profit_total"),
        ),
        CardSpec(
            key=f"{marketplace}__06_average_order_value",
            name="Средний чек",
            display="scalar",
            tab=tab,
            row=1,
            col=20,
            size_x=4,
            size_y=3,
            sql=scalar_avg_sql(order_where),
            template_tags=GENERAL_COMMON_TAGS,
            visualization_settings=currency_scalar_settings("average_order_value"),
        ),
        CardSpec(
            key=f"{marketplace}__07_units_by_dates",
            name="Количество купленных товаров по датам",
            display="bar",
            tab=tab,
            row=4,
            col=0,
            size_x=12,
            size_y=6,
            sql=platform_units_by_dates_sql(marketplace),
            template_tags={"order_date": ITEMS_DATE_TAG, "status": ITEMS_STATUS_TAG, "granularity": GRANULARITY_TAG},
            visualization_settings=STATUS_TIMESERIES_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__08_profit_commissions",
            name=f"Прибыль и комиссии {label} по заказам",
            display="combo",
            tab=tab,
            row=4,
            col=12,
            size_x=12,
            size_y=6,
            sql=platform_profit_and_commissions_sql(marketplace),
            template_tags={**PLATFORM_PROFIT_TAGS, "granularity": GRANULARITY_TAG},
            visualization_settings=PROFIT_COMMISSION_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__09_commission_shares",
            name="Доли комиссий от выручки",
            display="area",
            tab=tab,
            row=11,
            col=0,
            size_x=24,
            size_y=6,
            sql=platform_commission_shares_sql(marketplace),
            template_tags={**PLATFORM_PROFIT_TAGS, "granularity": GRANULARITY_TAG},
            visualization_settings=COMMISSION_SHARE_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__10_status_split",
            name="Статусы заказов по месяцам",
            display="bar",
            tab=tab,
            row=18,
            col=0,
            size_x=16,
            size_y=6,
            sql=platform_status_split_sql(marketplace),
            template_tags={**GENERAL_COMMON_TAGS, "granularity": GRANULARITY_TAG},
            visualization_settings=STATUS_SPLIT_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__11_status_pie",
            name="Статусы заказов - кольцо",
            display="pie",
            tab=tab,
            row=18,
            col=16,
            size_x=8,
            size_y=6,
            sql=platform_status_pie_sql(marketplace),
            template_tags=GENERAL_COMMON_TAGS,
            visualization_settings=STATUS_PIE_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__12_fee_stats",
            name="Статистика по комиссиям",
            display="table",
            tab=tab,
            row=25,
            col=0,
            size_x=16,
            size_y=7,
            sql=platform_fee_stats_sql(marketplace),
            template_tags={"happened_at": FINANCE_DATE_TAG},
            visualization_settings=FEE_TABLE_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__13_warehouse_distribution",
            name="Распределение заказов по складам",
            display="pie",
            tab=tab,
            row=25,
            col=16,
            size_x=8,
            size_y=7,
            sql=platform_warehouse_distribution_sql(marketplace),
            template_tags=GENERAL_COMMON_TAGS,
            visualization_settings=WAREHOUSE_PIE_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__14_orders_table",
            name="Таблица заказов",
            display="table",
            tab=tab,
            row=33,
            col=0,
            size_x=24,
            size_y=9,
            sql=platform_orders_table_sql(marketplace),
            template_tags=PLATFORM_ORDER_TABLE_TAGS,
            inline_parameter_names=["Поиск по ID"],
        ),
        CardSpec(
            key=f"{marketplace}__15_fee_timeseries",
            name=f"Дополнительные услуги {label}",
            display="area",
            tab=tab,
            row=45,
            col=0,
            size_x=24,
            size_y=7,
            sql=platform_fee_name_timeseries_sql(marketplace),
            template_tags={"happened_at": FINANCE_DATE_TAG, "granularity": GRANULARITY_TAG},
            visualization_settings=FEE_TIMESERIES_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__16_periodic_commissions",
            name="Периодические комиссии",
            display="table",
            tab=tab,
            row=54,
            col=0,
            size_x=24,
            size_y=8,
            sql=platform_periodic_commissions_sql(marketplace),
            template_tags={"happened_at": FINANCE_DATE_TAG},
            visualization_settings=PERIODIC_COMMISSIONS_VISUALS,
        ),
        CardSpec(
            key=f"{marketplace}__17_stock_analysis",
            name="Анализ по остаткам",
            display="table",
            tab=tab,
            row=64,
            col=0,
            size_x=24,
            size_y=7,
            sql=platform_stock_analysis_sql(marketplace),
            template_tags=PLATFORM_STOCK_TAGS,
            inline_parameter_names=["Количество дней для анализа", "Склад инв."],
        ),
        CardSpec(
            key=f"{marketplace}__18_month_inventory_sales",
            name="Инвентаризация - продажи за месяц",
            display="table",
            tab=tab,
            row=73,
            col=0,
            size_x=24,
            size_y=7,
            sql=platform_month_inventory_sales_sql(marketplace),
            template_tags=PLATFORM_MONTH_INVENTORY_TAGS,
            inline_parameter_names=["Месяц инв.", "Склад инв."],
        ),
        CardSpec(
            key=f"{marketplace}__19_data_freshness",
            name="Свежесть данных",
            display="table",
            tab=tab,
            row=82,
            col=0,
            size_x=24,
            size_y=5,
            sql=f"""
SELECT
  INITCAP(REPLACE(marketplace, '_', ' ')) AS marketplace,
  last_order_at,
  last_finance_at,
  last_ads_at,
  last_stock_at,
  last_metric_at,
  orders_rows,
  finance_rows,
  ads_rows,
  stock_rows,
  metric_rows
FROM public.marketplace_data_freshness
WHERE marketplace = '{marketplace}';
""".strip(),
        ),
    ]


PLATFORM_CARD_SPECS = (
    platform_card_specs("yandex_market", "Яндекс Маркет", "Яндекс Маркет")
    + platform_card_specs("wildberries", "Wildberries", "Wildberries")
)


def load_card_specs() -> list[CardSpec]:
    return GENERAL_CARD_SPECS + list(PLATFORM_CARD_SPECS)


def generated_platform_virtual_dashcards(tab_ids: dict[str, int], starting_dashcard_id: int) -> tuple[list[dict[str, Any]], int]:
    dashcards: list[dict[str, Any]] = []
    next_dashcard_id = starting_dashcard_id
    for tab_name, label in (("Яндекс Маркет", "Яндекс Маркет"), ("Wildberries", "Wildberries")):
        tab_id = tab_ids[tab_name]
        for row, text, kind in [
            (0, f"Аналитика по заказам на {label}", "heading"),
            (10, "Детализация комиссий", "heading"),
            (17, "Статусы заказов", "heading"),
            (24, "Группировки", "heading"),
            (43, "Комиссии по услугам и заказам", "heading"),
            (53, "Периодические комиссии", "heading"),
            (63, "Анализ остатков", "heading"),
            (72, "Инвентаризация", "heading"),
        ]:
            if kind == "heading":
                dashcards.append(section_heading_dashcard(tab_id, text, next_dashcard_id, row))
            next_dashcard_id -= 1
        dashcards.append(
            info_dashcard(
                tab_id,
                "Некоторые ozon-специфичные блоки на этих вкладках заменены ближайшими честными аналогами на данных площадки.",
                next_dashcard_id,
                42,
            )
        )
        next_dashcard_id -= 1
    return dashcards, next_dashcard_id


def generated_general_virtual_dashcards(tab_ids: dict[str, int], starting_dashcard_id: int) -> tuple[list[dict[str, Any]], int]:
    tab_id = tab_ids["Общее"]
    dashcards: list[dict[str, Any]] = []
    next_dashcard_id = starting_dashcard_id
    for row, text in [
        (0, "Аналитика по всем площадкам"),
        (25, "Повторные заказы"),
        (33, "Группировки"),
        (41, "Таблица заказов"),
        (52, "Комиссии по категориям"),
        (60, "Дополнительные услуги"),
        (68, "Периодические комиссии"),
        (78, "Инвентаризация"),
    ]:
        dashcards.append(section_heading_dashcard(tab_id, text, next_dashcard_id, row))
        next_dashcard_id -= 1
    return dashcards, next_dashcard_id


def remap_dashcard_parameter_mappings(
    mappings: list[dict[str, Any]],
    *,
    source_parameter_names: dict[str, str],
    target_parameter_ids: dict[str, str],
    card_id: int | None,
) -> list[dict[str, Any]]:
    remapped: list[dict[str, Any]] = []
    for mapping in mappings:
        source_parameter_id = mapping.get("parameter_id")
        parameter_name = source_parameter_names.get(source_parameter_id)
        target_parameter_id = target_parameter_ids.get(parameter_name) if parameter_name else None
        if not target_parameter_id:
            continue
        remapped.append(
            {
                **mapping,
                "parameter_id": target_parameter_id,
                "card_id": card_id,
            }
        )
    return remapped


def remap_inline_parameters(
    inline_parameters: list[str],
    *,
    source_parameter_names: dict[str, str],
    target_parameter_ids: dict[str, str],
) -> list[str]:
    remapped: list[str] = []
    for source_parameter_id in inline_parameters:
        parameter_name = source_parameter_names.get(source_parameter_id)
        target_parameter_id = target_parameter_ids.get(parameter_name) if parameter_name else None
        if target_parameter_id:
            remapped.append(target_parameter_id)
    return remapped


def build_ozon_dashcards_from_source(
    *,
    source_dashboard: dict[str, Any],
    source_clone_ids: dict[int, int],
    target_parameter_ids: dict[str, str],
    starting_dashcard_id: int,
    target_tab_id: int,
) -> tuple[list[dict[str, Any]], int]:
    source_tabs = {tab["id"]: tab["name"] for tab in source_dashboard.get("tabs", [])}
    source_parameter_names = {
        param["id"]: param["name"]
        for param in source_dashboard.get("parameters", [])
        if param.get("id") and param.get("name")
    }
    tab_order = ["Аналитика заказов", "Таблица заказов", "Затраты", "Анализ"]
    tab_offsets: dict[str, int] = {}
    current_offset = 0
    dashcards = source_dashboard.get("dashcards", [])
    for tab_name in tab_order:
        tab_dashcards = [
            dashcard
            for dashcard in dashcards
            if source_tabs.get(dashcard.get("dashboard_tab_id")) == tab_name
        ]
        if not tab_dashcards:
            continue
        tab_offsets[tab_name] = current_offset
        current_offset += max(
            dashcard.get("row", 0) + dashcard.get("size_y", 0)
            for dashcard in tab_dashcards
        ) + 1

    copied_dashcards: list[dict[str, Any]] = []
    next_dashcard_id = starting_dashcard_id
    for tab_name in tab_order:
        for dashcard in sorted(
            (
                dashcard
                for dashcard in dashcards
                if source_tabs.get(dashcard.get("dashboard_tab_id")) == tab_name
            ),
            key=lambda item: (item.get("row", 0), item.get("col", 0), item.get("id", 0)),
        ):
            source_card_id = dashcard.get("card_id")
            card_id = source_clone_ids.get(source_card_id) if source_card_id else None
            payload = {
                "id": next_dashcard_id,
                "card_id": card_id,
                "row": dashcard.get("row", 0) + tab_offsets.get(tab_name, 0),
                "col": dashcard.get("col", 0),
                "size_x": dashcard.get("size_x", 24),
                "size_y": dashcard.get("size_y", 1),
                "inline_parameters": remap_inline_parameters(
                    dashcard.get("inline_parameters") or [],
                    source_parameter_names=source_parameter_names,
                    target_parameter_ids=target_parameter_ids,
                ),
                "parameter_mappings": remap_dashcard_parameter_mappings(
                    dashcard.get("parameter_mappings") or [],
                    source_parameter_names=source_parameter_names,
                    target_parameter_ids=target_parameter_ids,
                    card_id=card_id,
                ),
                "series": dashcard.get("series") or [],
                "dashboard_tab_id": target_tab_id,
            }
            if card_id is None:
                payload["visualization_settings"] = dashcard.get("visualization_settings") or {}
            copied_dashcards.append(payload)
            next_dashcard_id -= 1
    return copied_dashcards, next_dashcard_id


def save_sql_library(card_specs: list[CardSpec]) -> None:
    lines = [
        "# Marketplace Dashboard SQL",
        "",
        "SQL-запросы, которые используются для нового обзорного дашборда Metabase.",
        "",
        "## Naming",
        "",
        "- `general__NN_*.sql` - карточки вкладки `Общее`.",
        "- `ozon__NN_*.sql` - карточки вкладки `Ozon`.",
        "- `yandex_market__NN_*.sql` - карточки вкладки `Яндекс Маркет`.",
        "- `wildberries__NN_*.sql` - карточки вкладки `Wildberries`.",
        "",
        "## Files",
        "",
    ]
    for spec in card_specs:
        filename = f"{spec.key}.sql"
        write_text(SQL_CARDS_DIR / filename, spec.sql)
        lines.append(f"- `{filename}` -> `{spec.name}`")
    write_text(SQL_ROOT / "README.md", "\n".join(lines))


def create_or_update_card(
    client: MetabaseClient,
    existing_by_name: dict[str, dict[str, Any]],
    existing_by_key: dict[str, dict[str, Any]],
    spec: CardSpec,
) -> int:
    payload = {
        "name": spec.name,
        "display": spec.display,
        "dataset_query": {
            "lib/type": "mbql/query",
            "database": DATABASE_ID,
            "stages": [
                stage_with_tags(spec.sql, spec.template_tags)
            ],
        },
        "visualization_settings": spec.visualization_settings or {},
        "description": build_generated_description(spec),
    }
    existing = existing_by_key.get(spec.key)
    if existing is None and spec.name.startswith("Marketplace /"):
        existing = existing_by_name.get(spec.name)
    if existing:
        response = client.put(f"/api/card/{existing['id']}", payload)
        return response["id"]
    response = client.post("/api/card", payload)
    return response["id"]


def get_existing_dashboard_by_name(client: MetabaseClient, name: str) -> dict[str, Any] | None:
    dashboards = client.get("/api/dashboard")
    items = dashboards if isinstance(dashboards, list) else dashboards.get("data", [])
    for item in items:
        if item.get("name") == name:
            return client.get(f"/api/dashboard/{item['id']}")
    return None


def get_existing_cards_by_name(client: MetabaseClient) -> dict[str, dict[str, Any]]:
    cards = client.get("/api/card")
    items = cards if isinstance(cards, list) else cards.get("data", [])
    return {item.get("name"): item for item in items if item.get("name")}


def get_existing_generated_cards_by_key(client: MetabaseClient) -> dict[str, dict[str, Any]]:
    cards = client.get("/api/card")
    items = cards if isinstance(cards, list) else cards.get("data", [])
    generated: dict[str, dict[str, Any]] = {}
    for item in items:
        key = extract_generated_card_key(item.get("description"))
        if key:
            generated[key] = item
    return generated


def build_clone_description(source_card_id: int, original_description: str | None) -> str:
    marker = f"{OZON_CLONE_MARKER}{source_card_id}]"
    if original_description and original_description.strip():
        return f"{original_description.rstrip()}\n{marker}"
    return marker


def extract_clone_source_card_id(description: str | None) -> int | None:
    if not description:
        return None
    marker_start = description.rfind(OZON_CLONE_MARKER)
    if marker_start == -1:
        return None
    marker_end = description.find("]", marker_start)
    if marker_end == -1:
        return None
    raw = description[marker_start + len(OZON_CLONE_MARKER):marker_end]
    return int(raw) if raw.isdigit() else None


def get_existing_ozon_clone_cards(client: MetabaseClient) -> dict[int, dict[str, Any]]:
    cards = client.get("/api/card")
    items = cards if isinstance(cards, list) else cards.get("data", [])
    clones: dict[int, dict[str, Any]] = {}
    for item in items:
        source_card_id = extract_clone_source_card_id(item.get("description"))
        if source_card_id is not None:
            clones[source_card_id] = item
    return clones


def create_or_update_source_clone_card(
    client: MetabaseClient,
    *,
    source_card: dict[str, Any],
    existing_clone: dict[str, Any] | None,
) -> int:
    payload = {
        "name": source_card["name"],
        "display": source_card["display"],
        "dataset_query": source_card["dataset_query"],
        "visualization_settings": source_card.get("visualization_settings") or {},
        "description": build_clone_description(source_card["id"], source_card.get("description")),
    }
    if existing_clone:
        response = client.put(f"/api/card/{existing_clone['id']}", payload)
        return response["id"]
    response = client.post("/api/card", payload)
    return response["id"]


def ensure_ozon_clone_cards(
    client: MetabaseClient,
    *,
    source_dashboard: dict[str, Any],
) -> dict[int, int]:
    existing_clones = get_existing_ozon_clone_cards(client)
    clone_ids: dict[int, int] = {}
    source_cards: dict[int, dict[str, Any]] = {}
    for dashcard in source_dashboard.get("dashcards", []):
        card = dashcard.get("card")
        card_id = dashcard.get("card_id")
        if card_id and isinstance(card, dict) and card.get("dataset_query"):
            source_cards[card_id] = card
    for source_card_id, source_card in sorted(source_cards.items()):
        clone_ids[source_card_id] = create_or_update_source_clone_card(
            client,
            source_card=source_card,
            existing_clone=existing_clones.get(source_card_id),
        )
    return clone_ids


def build_parameter_mappings(spec: CardSpec, parameter_ids: dict[str, str]) -> list[dict[str, Any]]:
    if not spec.template_tags:
        return []

    mappings: list[dict[str, Any]] = []
    dimension_tags = {
        "order_date": "Дата",
        "status": "Исключить статусы",
        "flavor": "Вкус",
        "grams": "Граммовки",
        "marketplace_name": "Площадка",
        "items_marketplace": "Площадка",
        "finance_marketplace": "Площадка",
        "ads_marketplace": "Площадка",
        "stock_marketplace": "Площадка",
        "happened_at": "Дата",
        "stat_date": "Дата",
        "warehouse_name": "Склад инв.",
        "month": "Месяц инв.",
    }
    variable_tags = {
        "granularity": "Детализация",
        "order_id_search": "Поиск по ID",
        "days_back": "Количество дней для анализа",
    }

    for tag_name, parameter_name in dimension_tags.items():
        if tag_name in spec.template_tags and parameter_name in parameter_ids:
            mappings.append(
                {
                    "parameter_id": parameter_ids[parameter_name],
                    "card_id": None,
                    "target": [
                        "dimension",
                        ["template-tag", tag_name],
                        {"stage-number": 0},
                    ],
                }
            )

    for tag_name, parameter_name in variable_tags.items():
        if tag_name in spec.template_tags and parameter_name in parameter_ids:
            mappings.append(
                {
                    "parameter_id": parameter_ids[parameter_name],
                    "card_id": None,
                    "target": ["variable", ["template-tag", tag_name]],
                }
            )

    return mappings


def build_inline_parameters(spec: CardSpec, parameter_ids: dict[str, str]) -> list[str]:
    if not spec.inline_parameter_names:
        return []
    return [parameter_ids[name] for name in spec.inline_parameter_names if name in parameter_ids]


def build_dashboard_parameters(source_parameters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    params = [dict(param) for param in source_parameters]
    if not any(param.get("name") == "Площадка" for param in params):
        params.append(
            {
                "name": "Площадка",
                "slug": "marketplace",
                "id": "marketplace_filter",
                "type": "string/=",
                "sectionId": "string",
                "values_query_type": "list",
                "values_source_type": "static-list",
                "isMultiSelect": True,
                "default": ["ozon", "yandex_market", "wildberries"],
                "values_source_config": {
                    "values": [
                        ["ozon", "Ozon"],
                        ["yandex_market", "Яндекс Маркет"],
                        ["wildberries", "Wildberries"],
                    ]
                },
            }
        )
    return params


def dashboard_payload(
    *,
    new_card_ids: dict[str, int],
    parameter_ids: dict[str, str],
    source_dashboard: dict[str, Any],
    source_clone_ids: dict[int, int],
) -> dict[str, Any]:
    tab_ids = {
        "Общее": -1,
        "Ozon": -2,
        "Яндекс Маркет": -3,
        "Wildberries": -4,
    }
    cards: list[dict[str, Any]] = []
    next_dashcard_id = -1

    general_virtual_dashcards, next_dashcard_id = generated_general_virtual_dashcards(tab_ids, next_dashcard_id)
    cards.extend(general_virtual_dashcards)

    generated_virtual_dashcards, next_dashcard_id = generated_platform_virtual_dashcards(tab_ids, next_dashcard_id)
    cards.extend(generated_virtual_dashcards)

    for spec in load_card_specs():
        cards.append(
            {
                "id": next_dashcard_id,
                "card_id": new_card_ids[spec.key],
                "row": spec.row,
                "col": spec.col,
                "size_x": spec.size_x,
                "size_y": spec.size_y,
                "inline_parameters": build_inline_parameters(spec, parameter_ids),
                "parameter_mappings": [
                    {**mapping, "card_id": new_card_ids[spec.key]}
                    for mapping in build_parameter_mappings(spec, parameter_ids)
                ],
                "series": [],
                "dashboard_tab_id": tab_ids[spec.tab],
            }
        )
        next_dashcard_id -= 1

    ozon_dashcards, next_dashcard_id = build_ozon_dashcards_from_source(
        source_dashboard=source_dashboard,
        source_clone_ids=source_clone_ids,
        target_parameter_ids=parameter_ids,
        starting_dashcard_id=next_dashcard_id,
        target_tab_id=tab_ids["Ozon"],
    )
    cards.extend(ozon_dashcards)

    return {
        "tabs": [
            {"id": tab_ids["Общее"], "name": "Общее"},
            {"id": tab_ids["Ozon"], "name": "Ozon"},
            {"id": tab_ids["Яндекс Маркет"], "name": "Яндекс Маркет"},
            {"id": tab_ids["Wildberries"], "name": "Wildberries"},
        ],
        "cards": cards,
    }


def ensure_target_dashboard(client: MetabaseClient) -> dict[str, Any]:
    existing = get_existing_dashboard_by_name(client, TARGET_DASHBOARD_NAME)
    if existing:
        return existing
    created = client.post(
        "/api/dashboard",
        {
            "name": TARGET_DASHBOARD_NAME,
            "description": "Review dashboard with General / Ozon / Yandex Market / Wildberries tabs.",
        },
    )
    return client.get(f"/api/dashboard/{created['id']}")


def build_dashboard() -> dict[str, Any]:
    ensure_env_loaded()
    api_key = os.getenv("METABASE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("METABASE_API_KEY is missing.")

    client = MetabaseClient(
        base_url=os.getenv("METABASE_URL", DEFAULT_METABASE_URL),
        api_key=api_key,
    )
    source_dashboard = client.get(f"/api/dashboard/{SOURCE_DASHBOARD_ID}")
    source_parameters = build_dashboard_parameters(source_dashboard.get("parameters", []))
    parameter_ids = {param["name"]: param["id"] for param in source_parameters if param.get("name") and param.get("id")}
    target_dashboard = ensure_target_dashboard(client)

    card_specs = load_card_specs()
    save_sql_library(card_specs)

    existing_by_name = get_existing_cards_by_name(client)
    existing_by_key = get_existing_generated_cards_by_key(client)
    new_card_ids: dict[str, int] = {}
    for spec in card_specs:
        new_card_ids[spec.key] = create_or_update_card(client, existing_by_name, existing_by_key, spec)
    source_clone_ids = ensure_ozon_clone_cards(client, source_dashboard=source_dashboard)

    payload = dashboard_payload(
        new_card_ids=new_card_ids,
        parameter_ids=parameter_ids,
        source_dashboard=source_dashboard,
        source_clone_ids=source_clone_ids,
    )
    client.put(
        f"/api/dashboard/{target_dashboard['id']}",
        {
            "name": TARGET_DASHBOARD_NAME,
            "description": "Review dashboard with General / Ozon / Yandex Market / Wildberries tabs. Detailed historical OZON dashboard remains separate.",
            "parameters": source_parameters,
        },
    )
    client.put(f"/api/dashboard/{target_dashboard['id']}/cards", payload)
    updated_dashboard = client.get(f"/api/dashboard/{target_dashboard['id']}")

    manifest = {
        "dashboard_id": updated_dashboard["id"],
        "dashboard_name": updated_dashboard["name"],
        "card_ids": new_card_ids,
        "tabs": [tab["name"] for tab in updated_dashboard.get("tabs", [])],
        "dashcards_count": len(updated_dashboard.get("dashcards", [])),
        "sql_root": str(SQL_ROOT.relative_to(BASE_DIR)),
    }
    write_json(SQL_ROOT / "manifest.json", manifest)
    return manifest


def main() -> int:
    try:
        manifest = build_dashboard()
    except Exception as exc:
        print(f"[build_marketplace_dashboard] error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
