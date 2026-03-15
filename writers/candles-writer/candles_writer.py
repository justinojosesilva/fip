import json
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
      
for event in kafka.consume("crypto.candles", "candles-writer"):
    candle = event["data"]
    print("Candle received: ", event)
    save_candle(candle)
    