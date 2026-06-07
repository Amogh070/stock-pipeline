FROM python:3.12-slim

WORKDIR /app

COPY docker/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producers/ ./producers/

CMD ["python", "producers/stock_producer.py"]
