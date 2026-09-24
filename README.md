# Real-Time Market Data Pipeline

A streaming data pipeline that ingests live stock ticks, processes them through
a message broker, lands them in a warehouse, transforms them with dbt, and
serves a live-updating dashboard.

## Architecture

```
Alpaca Market Data API (WebSocket)
        |
        v
  Producer (Python)  ---->  Redpanda / Kafka topic "stock-ticks"
        |
        v
  Consumer (Python)  ---->  Raw landing table (Postgres/Snowflake/BigQuery) + S3 archive
        |
        v
  dbt (staging -> marts)  ---->  5-min OHLC bars, moving averages
        |
        v
  Streamlit dashboard (auto-refreshing)
```

**Local dev stack :**
- Redpanda (Kafka-compatible broker) via Docker Compose
- Postgres as the warehouse stand-in 
- Alpaca free-tier market data

**Cloud swap path:**
- Redpanda -> Amazon Kinesis Data Streams
- Postgres -> Snowflake or BigQuery
- Add Kinesis Data Analytics or a Lambda consumer for the transform step

## Setup

1. Get a free Alpaca account and API keys: https://alpaca.markets/
2. Copy `.env.example` to `.env` and fill in your keys
3. Start the local broker + warehouse:
   ```
   docker-compose up -d
   ```
4. Install `uv` if you don't have it (one-time): `curl -LsSf https://astral.sh/uv/install.sh | sh` (Mac/Linux) or see https://docs.astral.sh/uv/getting-started/installation/ for Windows
5. Create the environment and install dependencies (reads `pyproject.toml`):
   ```
   uv sync
   ```
   This creates a `.venv` folder and installs everything — no separate `pip install` step, and no need to manually activate the venv for the commands below since `uv run` handles that.
6. Run the producer (streams live ticks into Redpanda):
   ```
   uv run producer/stock_producer.py
   ```
7. Run the consumer (writes ticks into Postgres + archives raw JSON to disk, standing in for S3):
   ```
   uv run consumer/kinesis_consumer.py
   ```
8. Run dbt to build staging + mart models:
   ```
   cd dbt_project && uv run dbt run
   ```
9. Launch the dashboard:
   ```
   uv run streamlit run dashboard/app.py
   ```

## Project layout

```
producer/           # Pulls ticks from Alpaca, publishes to Redpanda topic
consumer/           # Subscribes to topic, writes to warehouse + raw archive
dbt_project/         # staging -> marts transformations
dashboard/          # Streamlit app reading from marts
docker-compose.yml  # Redpanda + Postgres, local only
```

## Next steps

- Add a rolling moving-average / anomaly-flag mart (e.g. flag ticks >2 std dev from 5-min mean)
- Add dbt tests + a freshness check, and wire a Slack/email alert on failure
- Swap Redpanda -> Kinesis and Postgres -> Snowflake/BigQuery, document the migration in the README
- Containerize producer/consumer and deploy (ECS, Fargate, or even a small EC2 box) so the whole thing runs unattended
