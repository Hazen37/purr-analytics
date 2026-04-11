from src.etl.alerts.marketplace_health_alerts import run_marketplace_health_alerts


if __name__ == "__main__":
    run_marketplace_health_alerts(
        include_ozon=False,
        include_ym=True,
        include_wb=False,
        notify_ozon=False,
        notify_ym=True,
        notify_wb=False,
    )
