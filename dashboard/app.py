"""
Live-updating dashboard reading from the dbt mart tables.

Run: streamlit run dashboard/app.py
"""

import os
import time

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Live Market Pipeline", layout="wide")

REFRESH_SECONDS = 5


def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def load_ohlc(conn) -> pd.DataFrame:
    return pd.read_sql(
        "select * from mart_ohlc_5min order by bar_start desc limit 500", conn
    )


def load_anomalies(conn) -> pd.DataFrame:
    return pd.read_sql(
        """
        select * from mart_price_anomalies
        where is_anomaly = true
        order by exchange_ts desc
        limit 50
        """,
        conn,
    )


def load_latency(conn) -> pd.DataFrame:
    return pd.read_sql(
        """
        select
            symbol,
            avg(pipeline_latency_ms) as avg_latency_ms,
            percentile_cont(0.95) within group (order by pipeline_latency_ms)
                as p95_latency_ms,
            count(*) as tick_count
        from stg_stock_ticks
        group by symbol
        order by symbol
        """,
        conn,
    )


st.title("Real-Time Market Data Pipeline")
st.caption(f"Auto-refreshing every {REFRESH_SECONDS}s from dbt mart tables")

placeholder = st.empty()

while True:
    conn = get_connection()
    ohlc = load_ohlc(conn)
    anomalies = load_anomalies(conn)
    latency = load_latency(conn)
    conn.close()

    with placeholder.container():
        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("5-Minute OHLC Bars")
            if not ohlc.empty:
                for symbol in ohlc["symbol"].unique():
                    sym_df = ohlc[ohlc["symbol"] == symbol].sort_values("bar_start")
                    st.line_chart(
                        sym_df.set_index("bar_start")[["close_price"]],
                        height=200,
                    )
                    st.caption(symbol)
            else:
                st.info("Waiting for data... make sure the producer and consumer are running.")

        with col2:
            st.subheader("Pipeline Latency")
            st.dataframe(latency, use_container_width=True)

            st.subheader("Recent Anomalies")
            st.dataframe(anomalies, use_container_width=True)

    time.sleep(REFRESH_SECONDS)
