import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "RAW_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

JARS = ",".join([
    "jars/spark-sql-kafka.jar",
    "jars/kafka-clients.jar",
    "jars/spark-token-provider.jar",
    "jars/commons-pool2.jar"
])

SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("ticker", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType())
])

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE
    )

def write_to_snowflake(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS STOCK_PRICES_RAW (
                symbol VARCHAR,
                ticker VARCHAR,
                event_timestamp TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for row in rows:
            cursor.execute("""
                INSERT INTO STOCK_PRICES_RAW
                (symbol, ticker, event_timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.symbol, row.ticker, row.event_timestamp,
                row.open, row.high, row.low, row.close, row.volume
            ))
        conn.commit()
        logger.info(f"Batch {batch_id}: wrote {len(rows)} rows to Snowflake")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Snowflake write error: {e}")
        raise

def main():
    spark = SparkSession.builder \
        .appName("StockPriceStreaming") \
        .config("spark.jars", JARS) \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed = df.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*") \
     .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
     .drop("timestamp")

    query = parsed.writeStream \
        .foreachBatch(write_to_snowflake) \
        .option("checkpointLocation", "checkpoints/stock_stream") \
        .trigger(processingTime="30 seconds") \
        .start()

    logger.info("Streaming job started. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
