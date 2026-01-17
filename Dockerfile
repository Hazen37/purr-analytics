FROM python:3.11-slim

WORKDIR /app

# зависимости
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# код
COPY . /app

# контейнер просто живёт, ETL дергается отдельно (cron / exec)
CMD ["sleep", "infinity"]