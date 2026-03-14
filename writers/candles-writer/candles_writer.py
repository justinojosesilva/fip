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
                "crypto.candles",
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


consumer = connect_kafkaConsumer()

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

def save_candle(candle):
    try:
      time_value = datetime.fromisoformat(candle["time"])
      cursor.execute(
          """
          INSERT INTO market_data.crypto_candles
          (time, symbol, interval, open, high, low, close, volume, exchange)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON CONFLICT DO NOTHING
          """,
          (
              time_value,
              candle["symbol"],
              candle["interval"],
              candle["open"],
              candle["high"],
              candle["low"],
              candle["close"],
              candle["volume"],
              config.EXCHANGE
          )
      )
      conn.commit()
    except Exception as e:
      print(f"Error saving candle: {e}")
      conn.rollback()
      
for message in consumer:
    candle = message.value
    print("Candle received: ", candle)
    save_candle(candle)
    