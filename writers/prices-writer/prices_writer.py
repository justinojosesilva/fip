import json
import time
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import config

BATCH_SIZE = 100
FLUSH_INTERVAL = 2
buffer = []
last_flush = time.time()

# kafka Consumer
def connect_kafkaConsumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "crypto.prices",
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
        
for message in consumer:
    data = message.value
    
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