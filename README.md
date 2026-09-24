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

**Local dev stack (free, $0):**
- Redpanda (Kafka-compatible broker) via Docker Compose — no Zookeeper, no AWS bill
- Postgres as the warehouse stand-in (swap for Snowflake/BigQuery later, dbt makes this a config change, not a rewrite)
- Alpaca free-tier market data (IEX feed, real but slightly delayed ticks — enough for this project)

**Cloud swap path (for the resume bullet "deployed to AWS"):**
- Redpanda -> Amazon Kinesis Data Streams
- Postgres -> Snowflake or BigQuery
- Add Kinesis Data Analytics or a Lambda consumer for the transform step

## Setup

1. Get a free Alpaca account and API keys: https://alpaca.markets/ (paper trading / market data, no funding needed)
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

## What to measure / write up (for your resume + interviews)

- **End-to-end latency**: timestamp the moment a tick arrives from Alpaca vs. the moment it appears on the dashboard. Log this and report a p50/p95 number.
- **Throughput**: ticks/sec the pipeline sustains without consumer lag.
- **A failure you handled**: e.g. what happens if the consumer goes down for 60 seconds — does the broker buffer, do you lose data, how do you detect and recover? Kill the consumer on purpose and document it.
- **Data quality**: add dbt tests (not_null, unique, freshness) on the staging model and note what they catch.

## Project layout

```
producer/           # Pulls ticks from Alpaca, publishes to Redpanda topic
consumer/           # Subscribes to topic, writes to warehouse + raw archive
dbt_project/         # staging -> marts transformations
dashboard/          # Streamlit app reading from marts
docker-compose.yml  # Redpanda + Postgres, local only
```

## Next steps once this works end-to-end

- Add a rolling moving-average / anomaly-flag mart (e.g. flag ticks >2 std dev from 5-min mean)
- Add dbt tests + a freshness check, and wire a Slack/email alert on failure
- Swap Redpanda -> Kinesis and Postgres -> Snowflake/BigQuery, document the migration in the README
- Containerize producer/consumer and deploy (ECS, Fargate, or even a small EC2 box) so the whole thing runs unattended
