"""
Producer: streams live stock trades from Alpaca's WebSocket market data feed
and publishes each tick as a JSON message to a Kafka/Redpanda topic.

Run: python producer/stock_producer.py
"""

import os
import json
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
SYMBOLS = os.environ["SYMBOLS"].split(",")
API_KEY = os.environ["ALPACA_API_KEY"]
SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed for message: {err}")


async def on_trade(trade):
    """Callback fired for every incoming trade tick."""
    event = {
        "symbol": trade.symbol,
        "price": float(trade.price),
        "size": int(trade.size),
        "exchange_timestamp": trade.timestamp.isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    producer.produce(
        TOPIC,
        key=event["symbol"],
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.poll(0)  # trigger delivery callbacks without blocking
    log.info(f"Published tick: {event['symbol']} @ {event['price']}")


def main():
    log.info(f"Starting stream for symbols: {SYMBOLS}")
    stream = StockDataStream(API_KEY, SECRET_KEY, feed=DataFeed.IEX)  # free tier feed
    stream.subscribe_trades(on_trade, *SYMBOLS)

    try:
        stream.run()
    except KeyboardInterrupt:
        log.info("Shutting down producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
