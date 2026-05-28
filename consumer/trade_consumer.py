import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer
import clickhouse_connect

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw")
KAFKA_TOPIC_DEAD_LETTER = os.getenv("KAFKA_TOPIC_DEAD_LETTER", "crypto.trades.dead_letter")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "crypto")
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "crypto-trade-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

dead_letter_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

clickhouse_client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USERNAME,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE
)


def millis_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def send_dead_letter(raw_event, error_message):
    dead_event = {
        "raw_event": raw_event,
        "error_message": error_message
    }

    dead_letter_producer.produce(
        topic=KAFKA_TOPIC_DEAD_LETTER,
        value=json.dumps(dead_event).encode("utf-8")
    )
    dead_letter_producer.flush()

    clickhouse_client.insert(
        "dead_letter_events",
        [[json.dumps(raw_event), error_message]],
        column_names=["raw_event", "error_message"]
    )


def transform_event(raw_message):
    event = json.loads(raw_message)

    # Binance combined stream có dạng:
    # {
    #   "stream": "btcusdt@trade",
    #   "data": {...}
    # }
    payload = event.get("data", event)

    required_fields = ["E", "s", "t", "p", "q", "T", "m"]

    for field in required_fields:
        if field not in payload:
            raise ValueError(f"Missing required field: {field}")

    event_time = millis_to_datetime(payload["E"])
    trade_time = millis_to_datetime(payload["T"])
    symbol = payload["s"]
    trade_id = int(payload["t"])
    price = float(payload["p"])
    quantity = float(payload["q"])
    trade_value = price * quantity
    is_buyer_market_maker = 1 if payload["m"] else 0

    return [
        event_time,
        trade_time,
        symbol,
        trade_id,
        price,
        quantity,
        trade_value,
        is_buyer_market_maker
    ]


def main():
    consumer.subscribe([KAFKA_TOPIC_RAW])

    print("[START] Kafka Consumer")
    print(f"[KAFKA] {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[TOPIC] {KAFKA_TOPIC_RAW}")
    print(f"[CLICKHOUSE] {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"[CONSUMER ERROR] {msg.error()}")
            continue

        raw_message = msg.value().decode("utf-8")

        try:
            row = transform_event(raw_message)

            clickhouse_client.insert(
                "trades_clean",
                [row],
                column_names=[
                    "event_time",
                    "trade_time",
                    "symbol",
                    "trade_id",
                    "price",
                    "quantity",
                    "trade_value",
                    "is_buyer_market_maker"
                ]
            )

            consumer.commit(msg)

            print(
                f"[INSERTED] symbol={row[2]} "
                f"price={row[4]} qty={row[5]} value={row[6]}"
            )

        except Exception as e:
            print(f"[INVALID EVENT] {e}")
            send_dead_letter(raw_message, str(e))
            consumer.commit(msg)


if __name__ == "__main__":
    main()