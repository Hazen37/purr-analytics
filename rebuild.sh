docker-compose build etl_runner
docker-compose run --rm etl_runner python -u -m src.cli.update_all