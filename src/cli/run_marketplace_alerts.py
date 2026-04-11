from src.etl.alerts.marketplace_health_alerts import run_marketplace_health_alerts


if __name__ == "__main__":
    run_marketplace_health_alerts(
        include_ozon=True,
        include_ym=True,
        include_wb=True,
        notify_ozon=True,
        notify_ym=True,
        notify_wb=True,
    )
