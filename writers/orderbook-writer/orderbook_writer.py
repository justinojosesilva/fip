import psycopg2
import time
import config
from common.kafka import KafkaClient
from datetime import datetime

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