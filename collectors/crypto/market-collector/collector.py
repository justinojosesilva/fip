import json
from statistics import quantiles
import websocket
import psycopg2
import time
from kafka import KafkaProducer
from datetime import datetime, timezone
import config

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

# kafka producer
def connect_kafkaProducer():
  while True:
    try:
      producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
      )
      print("Connected to Kafka")
      return producer
    except Exception as e:
      print("Kafka not ready, retrying in 5s...")
      time.sleep(5)

producer = connect_kafkaProducer()

def save_price(symbol, price):
    try:
      cursor.execute(
        """
        INSERT INTO market_data.crypto_prices(time, symbol, price)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (datetime.now(timezone.utc), symbol, price)
      )

      conn.commit()
    except Exception as e:
      print("Database error: ", e)
      conn.rollback()
      
def save_trade(symbol, trade_id, price, quantity, side, trade_time):
    try:
      cursor.execute(
        """
        INSERT INTO market_data.crypto_trades_raw(time, symbol, trade_id, price, quantity, side, exchange)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (trade_time, symbol, trade_id, price, quantity, side, config.EXCHANGE)
      )

      conn.commit()
    except Exception as e:
      print("Database error: ", e)
      conn.rollback()  

def on_message(ws, message):
  data = json.loads(message)
  
  symbol = data["s"]
  trade_id = data["t"]

  price = float(data["p"])
  quantity = float(data["q"])
  
  timestamp = data["T"]
  is_maker = data["m"]
  
  trade_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
  
  side = "sell" if is_maker else "buy"

  event = {
    "symbol": symbol,
    "price": price,
    "quantity": quantity,
    "side": side,
    "trade_id": trade_id,
    "time": trade_time.isoformat()
  }
  
  event_price = {
    "symbol": symbol,
    "price": price,
    "time": trade_time.isoformat()
  }

  # envia para kafka
  producer.send("crypto.trades", event)
  producer.send("crypto.prices", event_price)

  # salva a trade no banco
  save_trade(symbol, trade_id, price, quantity, side, trade_time)

  # salva no banco
  save_price(symbol, price)

  print(event)

def on_error(ws, error):
  print(error)

def on_close(ws, close_status_code, close_msg):
  print("connection closed")

def on_open(ws):
  print("connected to Binance websocket")

if __name__ == "__main__":
  socket = f"wss://stream.binance.com:9443/ws/{config.SYMBOL}@trade"
  ws = websocket.WebSocketApp(
    socket,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
  )

  ws.on_open = on_open
  ws.run_forever()