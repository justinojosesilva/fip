import json
import time
from datetime import datetime
from tkinter import GROOVE

import config
from kafka import KafkaConsumer, KafkaProducer


# kafka Consumer
def connect_kafkaConsumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "crypto.trades",
                bootstrap_servers=config.KAFKA_SERVER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                ##group_id="candles-engine",
            )
            print("Connected to Kafka")
            return consumer
        except Exception as e:
            print("Kafka not ready, retrying in 5s...")
            time.sleep(5)


# kafka Producer
def connect_kafkaProducer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_SERVER,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            )
            print("Connected to Kafka Producer")
            return producer
        except Exception as e:
            print("Kafka Producer not ready, retrying in 5s...")
            time.sleep(5)


consumer = connect_kafkaConsumer()
producer = connect_kafkaProducer()

current_candle = None
current_window = None


def start_new_candle(price, qty, symbol):
    return {
        "symbol": symbol,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": qty,
    }


for message in consumer:
    trade = message.value

    symbol = trade["symbol"]
    price = float(trade["price"])
    qty = float(trade["quantity"])

    trade_time = datetime.fromisoformat(trade["time"]).timestamp()

    window = int(trade_time // config.CANDLE_INTERVAL)

    if current_window is None:
      
        current_window = window
        current_candle = start_new_candle(price, qty, symbol)

    elif window != current_window:
        candle_event = {
            "symbol": current_candle["symbol"],
            "interval": "1m",
            "open": current_candle["open"],
            "high": current_candle["high"],
            "low": current_candle["low"],
            "close": current_candle["close"],
            "volume": current_candle["volume"],
            "time": datetime.utcfromtimestamp(
                current_window * config.CANDLE_INTERVAL
            ).isoformat(),
        }

        producer.send("crypto.candles", candle_event)
        print("Candle: ", candle_event)

        current_window = window
        current_candle = start_new_candle(price, qty, symbol)

    else:
        current_candle["high"] = max(current_candle["high"], price)
        current_candle["low"] = min(current_candle["low"], price)
        current_candle["close"] = price
        current_candle["volume"] += qty
