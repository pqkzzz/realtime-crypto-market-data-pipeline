import os
import json
import time
from dotenv import load_dotenv
from websocket import WebSocketApp
from confluent_kafka import Producer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw")
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA ERROR] Delivery failed: {err}")
    else:
        print(
            f"[KAFKA OK] topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def on_open(ws):
    print("[WS CONNECTED] Connected to Binance WebSocket")


def on_message(ws, message):
    try:
        data = json.loads(message)

        producer.produce(
            topic=KAFKA_TOPIC_RAW,
            value=json.dumps(data).encode("utf-8"),
            callback=delivery_report
        )

        producer.poll(0)

        stream = data.get("stream")
        payload = data.get("data", {})
        symbol = payload.get("s")
        price = payload.get("p")
        quantity = payload.get("q")

        print(f"[PRODUCED] stream={stream} symbol={symbol} price={price} qty={quantity}")

    except Exception as e:
        print(f"[PRODUCER ERROR] {e}")


def on_error(ws, error):
    print(f"[WS ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[WS CLOSED] code={close_status_code}, msg={close_msg}")


def run_forever():
    while True:
        try:
            ws = WebSocketApp(
                BINANCE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(ping_interval=30, ping_timeout=10)

        except Exception as e:
            print(f"[RECONNECT ERROR] {e}")

        print("[RECONNECTING] Waiting 5 seconds...")
        time.sleep(5)


if __name__ == "__main__":
    print("[START] Binance WebSocket Producer")
    print(f"[KAFKA] {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[TOPIC] {KAFKA_TOPIC_RAW}")
    print(f"[BINANCE] {BINANCE_WS_URL}")

    run_forever()