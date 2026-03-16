import config
from common.base_kafka import BaseKafka
from common.db import Database
from datetime import datetime

kafka = BaseKafka(config.KAFKA_SERVER)
db = Database(config)

conn = db.connect()
cursor = conn.cursor()

def save_orderbook(event):
    now = datetime.fromisoformat(event["time"])
    try:
      cursor.execute(
          """
          INSERT INTO analytics.crypto_orderbook 
          (time, symbol, bid_volume, ask_volume, spread, imbalance)
          VALUES (%s, %s, %s, %s, %s, %s)
          ON CONFLICT DO NOTHING
          """,
          (
              now,
              event["symbol"],
              event["bid_volume"],
              event["ask_volume"],
              event["spread"],
              event["imbalance"]
          ),
      )
      conn.commit()
    except Exception as e:
      print(f"Error saving orderbook data: {e}")
      conn.rollback()
      

for event in kafka.consume("crypto.orderbook.metrics", "orderbook-writer"):
    data = event["data"]
    print("Saved orderbook metric: ", event)
    save_orderbook(data)