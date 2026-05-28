# Real-time Crypto Market Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue.png)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-black.png)
![ClickHouse](https://img.shields.io/badge/ClickHouse-OLAP-yellow.png)
![Grafana](https://img.shields.io/badge/Grafana-Dashboard-orange.png)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.png)
![Binance](https://img.shields.io/badge/Binance-WebSocket-yellowgreen.png)

Real-time streaming pipeline that ingests live Binance trades, pushes raw events to Kafka, validates/transforms them, stores clean data in ClickHouse, and visualizes metrics in Grafana.

## Architecture

```text
Binance WebSocket
        |
        v
Python Producer
        |
        v
Apache Kafka
        |
        v
Python Consumer
        |
        v
ClickHouse
        |
        v
Grafana Dashboard
```

## Tech Stack

- Python
- Apache Kafka
- ClickHouse
- Grafana
- Docker Compose

## Features

- Ingest live trade events from Binance WebSocket
- Publish raw events to Kafka topic `crypto.trades.raw`
- Validate and transform trade events
- Store clean data in ClickHouse
- Persist invalid events into a dead-letter topic and table

## Setup

1) Create a local env file from the template:

```bash
copy .env.example .env
```

2) Update any values in `.env` as needed (Kafka bootstrap, ClickHouse password, symbols).

## Run

Start infrastructure (ClickHouse, Grafana):

```bash
docker compose up -d
```

Run the producer and consumer:

```bash
python producer/binance_ws_producer.py
python consumer/trade_consumer.py
```

## Data Model

Tables are created on ClickHouse startup via [sql/create_clickhouse_tables.sql](sql/create_clickhouse_tables.sql).

### trades_clean

- `event_time`: event time from Binance (ms)
- `trade_time`: trade time from Binance (ms)
- `symbol`: trading pair
- `trade_id`: Binance trade id
- `price`: trade price
- `quantity`: trade quantity
- `trade_value`: price * quantity
- `is_buyer_market_maker`: 1 if buyer is market maker
- `ingested_at`: ClickHouse insert time

### dead_letter_events

- `event_time`: insert time
- `raw_event`: raw event payload
- `error_message`: error reason

## Configuration

All runtime settings are read from `.env` (see `.env.example`).

Key variables:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC_RAW`
- `KAFKA_TOPIC_DEAD_LETTER`
- `BINANCE_WS_URL`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_USERNAME`
- `CLICKHOUSE_PASSWORD`

## Notes

- `.env` is intentionally gitignored. Use `.env.example` as a template.
- If you change the ClickHouse password, keep `docker-compose.yml` and `.env` in sync.