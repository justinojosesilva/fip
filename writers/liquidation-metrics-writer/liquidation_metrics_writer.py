import psycopg2
import time
import config
from datetime import datetime
from common.base_kafka import BaseKafka
from common.db import Database

kafka = BaseKafka(config.KAFKA_SERVER)
db = Database(config)

conn = db.connect()
cursor = conn.cursor()

def save_metric(metric):
    try:
      time_value = datetime.fromisoformat(metric["time"])
      
      cursor.execute(
          """
          INSERT INTO analytics.crypto_liquidation_metrics
          (time, symbol, interval, long_liquidations, short_liquidations, liquidation_imbalance)
          VALUES (%s, %s, %s, %s, %s, %s)
          ON CONFLICT DO NOTHING
          """,
          (
              time_value,
              metric["symbol"],
              metric["interval"],
              metric["long_liquidations"],
              metric["short_liquidations"],
              metric["liquidation_imbalance"]
          )
      )
      conn.commit()
    except Exception as e:
      print(f"Database error: {e}")
      conn.rollback()
      
for event in kafka.consume("crypto.liquidation.metrics", "liquidation-metrics-writer"):
    metric = event["data"]
    print("Saved liquidation metric:", event)
    save_metric(metric)    