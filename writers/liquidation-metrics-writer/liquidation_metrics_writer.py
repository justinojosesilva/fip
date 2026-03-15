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
                "crypto.liquidation.metrics",
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
      
for message in consumer:
    event = message.value
    metric = event["data"]
    print("Saved liquidation metric:", event)
    save_metric(metric)    