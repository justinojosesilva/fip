import config
from datetime import datetime
from common.base_kafka import BaseKafka
from common.db import Database

kafka = BaseKafka(config.KAFKA_SERVER)
db = Database(config)

conn = db.connect()
cursor = conn.cursor()


def save_indicator(event):

    try:

        time_value = datetime.fromisoformat(event["time"])

        cursor.execute(
            """
            INSERT INTO analytics.crypto_indicators
            (time, symbol, price, ema, rsi, vwap, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                time_value,
                event["symbol"],
                event["price"],
                event["ema"],
                event["rsi"],
                event["vwap"],
                event["volume"]
            )
        )

        conn.commit()

    except Exception as e:

        print("Database error:", e)
        conn.rollback()


for event in kafka.consume("crypto.micro.indicators", "micro-indicators-writer"):
    data = event["data"]
    print("Saved indicators:", event)
    save_indicator(data)