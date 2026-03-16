import time
import psycopg2
import config
from common.base_kafka import BaseKafka
from common.db import Database
from datetime import datetime

kafka = BaseKafka(config.KAFKA_SERVER)
db = Database(config)


BATCH_SIZE = 100
FLUSH_INTERVAL = 2
buffer = []
last_flush = time.time()

conn = db.connect()
cursor = conn.cursor()

def flush_buffer():
    global buffer, last_flush
    if not buffer:
        return  
    
    try:
        cursor.executemany(
            """
            INSERT INTO market_data.crypto_prices(time, symbol, price, exchange)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            buffer
        )
        conn.commit()
        print(f"Inserted {len(buffer)} prices to database")
        buffer = []
        last_flush = time.time()
    except Exception as e:
        print("Database error: ", e)
        conn.rollback()  
        
for event in kafka.consume("crypto.prices", "prices-writer"):
    data = event["data"]

    if not isinstance(data, dict):
        print("Invalid message format: ", data)
        continue
    
    try:
      price_time = datetime.fromisoformat(data["time"])
      
      row = (price_time, data["symbol"], data["price"], data["exchange"])
      buffer.append(row)
    except Exception as e:
      print("Invalid price message: ", data, e)
      
    if len(buffer) >= BATCH_SIZE:
        flush_buffer()
        
    if time.time() - last_flush >= FLUSH_INTERVAL:
        flush_buffer()