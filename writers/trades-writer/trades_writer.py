import time
import psycopg2
import config
from common.kafka import KafkaClient
from datetime import datetime

kafka = KafkaClient(config.KAFKA_SERVER)


BATCH_SIZE = 100
FLUSH_INTERVAL = 2  # seconds
buffer = []
last_flush = time.time()

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
            INSERT INTO market_data.crypto_trades_raw(time, symbol, trade_id, price, quantity, side, exchange)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            buffer
        )
        conn.commit()
        print(f"Flushed {len(buffer)} trades to database")
        buffer = []
        last_flush = time.time()
    except Exception as e:
        print("Database error during flush: ", e)
        conn.rollback()
    
for event in kafka.consume("crypto.trades", "trades-writer"):
    trade = event["data"]
    
    if not isinstance(trade, dict):
        print("Invalid message format: ", trade)
        continue
      
    try:
      trade_time = datetime.fromisoformat(trade["time"])
      
      row = (
        trade_time,
        trade["symbol"],
        trade["trade_id"],
        trade["price"],
        trade["quantity"],
        trade["side"],
        config.EXCHANGE
      )
      buffer.append(row)
    except Exception as e:
      print("Invalid trade: ", trade, "Error: ", e)
      continue
    
    if len(buffer) >= BATCH_SIZE:
        flush_buffer()
    
    if time.time() - last_flush >= FLUSH_INTERVAL:
        flush_buffer()