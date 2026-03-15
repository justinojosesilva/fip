import json
import psycopg2
import time
from kafka import KafkaConsumer
from datetime import datetime
import config

# kafka Consumer
def connect_kafkaConsumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "crypto.orderbook.metrics",
                bootstrap_servers=config.KAFKA_SERVER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            print("Connected to Kafka")
            return consumer
        except Exception as e:
            print("Kafka not ready, retrying in 5s...")
            time.sleep(5)

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
consumer = connect_kafkaConsumer()

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
      

for message in consumer:
    event = message.value
    data = event["data"]
    print("Saved orderbook metric: ", event)
    save_orderbook(data)