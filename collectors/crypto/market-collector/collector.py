import json
import websocket
import psycopg2
import time
from kafka import KafkaProducer
from datetime import datetime
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
    cursor.execute(
      """
      INSERT INTO crypto_prices(symbol, price, timestamp)
      VALUES(%s, %s, %s)
      """, 
      (symbol, price, datetime.utcnow())
    )

    conn.commit()

def on_message(ws, message):
  data = json.loads(message)

  price = float(data["p"])
  symbol = data["s"]

  event = {
    "symbol": symbol,
    "price": price,
    "timestamp": datetime.utcnow().isoformat()
  }

  # envia para kafka
  producer.send("crypto.prices", event)

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