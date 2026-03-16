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
            (
                time,
                symbol,
                interval,
                price,
                ema,
                rsi,
                vwap,
                macd,
                macd_signal,
                macd_histogram,
                bollinger_upper,
                bollinger_middle,
                bollinger_lower,
                volume
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                time_value,
                event["symbol"],
                event["interval"],
                event.get("price"),
                event.get("ema"),
                event.get("rsi"),
                event.get("vwap"),
                event.get("macd"),
                event.get("macd_signal"),
                event.get("macd_histogram"),
                event.get("bollinger_upper"),
                event.get("bollinger_middle"),
                event.get("bollinger_lower"),
                event.get("volume")
            )
        )
        conn.commit()
    except Exception as e:
        print("Database error:", e)
        conn.rollback()


for event in kafka.consume("crypto.indicators", "indicators-writer"):
    data = event["data"]
    print("Saving indicators:", data)
    save_indicator(data)