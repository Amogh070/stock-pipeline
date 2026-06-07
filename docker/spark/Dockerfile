FROM apache/spark:3.4.1

USER root

WORKDIR /app

COPY docker/spark/requirements-spark.txt .
RUN pip3 install --no-cache-dir -r requirements-spark.txt

COPY jobs/ ./jobs/
COPY jars/ ./jars/

CMD ["python3", "jobs/stream_stocks.py"]
