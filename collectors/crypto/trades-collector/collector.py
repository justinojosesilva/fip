import json
import websocket
import time
from kafka import KafkaProducer
from datetime import datetime, timezone
import config

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
    "time": trade_time.isoformat(),
    "exchange": config.EXCHANGE
  }
  
  event_price = {
    "symbol": symbol,
    "price": price,
    "time": trade_time.isoformat(),
    "exchange": config.EXCHANGE
  }

  # envia para kafka
  producer.send("crypto.trades", event)
  producer.send("crypto.prices", event_price)
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