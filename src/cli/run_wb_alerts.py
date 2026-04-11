from src.etl.alerts.marketplace_health_alerts import run_marketplace_health_alerts


if __name__ == "__main__":
    run_marketplace_health_alerts(
        include_ozon=False,
        include_ym=False,
        include_wb=True,
        notify_ozon=False,
        notify_ym=False,
        notify_wb=True,
    )
