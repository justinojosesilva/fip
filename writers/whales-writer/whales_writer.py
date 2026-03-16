import config
from datetime import datetime
from common.base_kafka import BaseKafka
from common.db import Database

kafka = BaseKafka(config.KAFKA_SERVER)
db = Database(config)

conn = db.connect()
cursor = conn.cursor()
  
def save_whale(event):
    try:
        time_value = datetime.fromisoformat(event["time"])
        cursor.execute(
            """
            INSERT INTO analytics.crypto_whales
            (time, symbol, price, quantity, trade_value, side)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                time_value,
                event["symbol"],
                event["price"],
                event["quantity"],
                event["value"],
                event["side"]
            )
        )
        conn.commit()

    except Exception as e:
        print("Database error:", e)
        conn.rollback()


for event in kafka.consume("crypto.whales.detected", "whales-writer"):
    data = event["data"]
    print("Saved whale:", event)
    save_whale(data)
    