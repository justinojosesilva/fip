import psycopg2
import time
import config
from datetime import datetime
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)

# conexão banco
def connect_db():
  while True:
    try:
      conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        database=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD
      )
      print("Connected to PostgreSQL")
      return conn
    except psycopg2.OperationalError:
      print("Waiting for PostgreSQL...")
      time.sleep(3)

conn = connect_db()
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