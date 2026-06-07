import json
import time
import logging
from datetime import datetime
from confluent_kafka import Producer
import yfinance as yf
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")

TICKERS = {
    "RELIANCE": "RELIANCE.NS",
    "TCS": "TCS.NS",
    "INFY": "INFY.NS",
    "HDFCBANK": "HDFCBANK.NS",
    "WIPRO": "WIPRO.NS"
}

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        logger.info(f"Delivered {msg.key().decode()} to {msg.topic()} [{msg.partition()}]")

def fetch_stock_data(symbol, ticker_code):
    ticker = yf.Ticker(ticker_code)
    data = ticker.history(period="1d", interval="1m")
    if data.empty:
        logger.warning(f"No data returned for {symbol}")
        return None
    latest = data.iloc[-1]
    return {
        "symbol": symbol,
        "ticker": ticker_code,
        "timestamp": datetime.utcnow().isoformat(),
        "open": round(float(latest["Open"]), 2),
        "high": round(float(latest["High"]), 2),
        "low": round(float(latest["Low"]), 2),
        "close": round(float(latest["Close"]), 2),
        "volume": int(latest["Volume"])
    }

def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    logger.info(f"Producer started. Sending to topic: {KAFKA_TOPIC}")

    while True:
        for symbol, ticker_code in TICKERS.items():
            try:
                record = fetch_stock_data(symbol, ticker_code)
                if record:
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=symbol,
                        value=json.dumps(record),
                        callback=delivery_report
                    )
                    logger.info(f"Queued: {record}")
            except Exception as e:
                logger.error(f"Error fetching {symbol}: {e}")

        producer.flush()
        logger.info("Sleeping 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    main()
