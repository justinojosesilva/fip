import config
from datetime import datetime
from common.base_kafka import BaseKafka
from common.db import Database

kafka = BaseKafka(config.KAFKA_SERVER)
db = Database(config)

conn = db.connect()
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
    