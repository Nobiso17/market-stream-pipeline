"""
Consumer: subscribes to the stock-ticks topic, writes each tick into the
Postgres warehouse (raw landing table) and archives the raw JSON to disk
(standing in for an S3 raw zone).

Named kinesis_consumer.py because this file is what you'd swap to boto3's
Kinesis client when you migrate off Redpanda -- the get_records loop below
maps directly onto Kinesis's shard-iterator pattern; only the client setup
changes.

Run: python consumer/kinesis_consumer.py
"""

import os
import json
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Consumer
import psycopg2

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
RAW_ARCHIVE_DIR = os.environ.get("RAW_ARCHIVE_DIR", "./raw_archive")

os.makedirs(RAW_ARCHIVE_DIR, exist_ok=True)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "market-consumer-group",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([TOPIC])

conn = psycopg2.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)
conn.autocommit = True


def ensure_table():
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_stock_ticks (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                price NUMERIC NOT NULL,
                size INTEGER NOT NULL,
                exchange_timestamp TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                consumed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )


def archive_raw(event: dict):
    """Append raw JSON to a per-day file, standing in for an S3 raw zone."""
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(RAW_ARCHIVE_DIR, f"{day}.jsonl")
    with open(path, "a") as f:
        f.write(json.dumps(event) + "\n")


def write_to_warehouse(event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_stock_ticks
                (symbol, price, size, exchange_timestamp, ingested_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                event["symbol"],
                event["price"],
                event["size"],
                event["exchange_timestamp"],
                event["ingested_at"],
            ),
        )


def main():
    ensure_table()
    log.info("Consumer started, waiting for messages...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            archive_raw(event)
            write_to_warehouse(event)

            # latency: time from ingestion at the producer to landing in the warehouse
            latency_ms = (
                datetime.now(timezone.utc)
                - datetime.fromisoformat(event["ingested_at"])
            ).total_seconds() * 1000
            log.info(
                f"Wrote {event['symbol']} @ {event['price']} "
                f"(ingest->warehouse latency: {latency_ms:.0f}ms)"
            )
    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
