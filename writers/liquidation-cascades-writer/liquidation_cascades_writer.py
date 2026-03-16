import config
from datetime import datetime
from common.base_kafka import BaseKafka
from common.db import Database

kafka = BaseKafka(config.KAFKA_SERVER)
db = Database(config)

conn = db.connect()
cursor = conn.cursor()

def save_cascade(event):
    try:
        time_value = datetime.fromisoformat(event["time"])
        
        cursor.execute(
            """
            INSERT INTO analytics.crypto_liquidation_cascades
            (time, symbol, type, volume)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                time_value,
                event["symbol"],
                event["type"],
                event["volume"]
            )
        )
        conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
        conn.rollback()
        
for event in kafka.consume("crypto.liquidation.cascades","liquidation-cascades-writer"):
    data = event["data"]
    print("Saved liquidation cascade:", event)
    save_cascade(data)
    