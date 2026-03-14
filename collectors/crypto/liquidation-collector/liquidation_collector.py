import json
from socket import socket
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
    
    # multiplex stream
    payload = data["data"]
    
    order = payload["o"]
    symbol = order["s"]
    side = order["S"]
    quantity = float(order["q"])
    price = float(order["ap"])
    
    timestamp = data["E"]
    event_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    
    liquidation_event = {
      "symbol": symbol,
      "side": side,
      "price": price,
      "quantity": quantity,
      "time": event_time.isoformat(),
      "exchange": config.EXCHANGE
    }
  
    print("Liquidation: ", liquidation_event) 
    producer.send("crypto.liquidations", liquidation_event)     
    
def on_error(ws, error):
  print("WebSocket error: ", error)

def on_close(ws, close_status_code, close_msg):
  print("WebSocket closed: ", close_status_code, close_msg)
  
def on_open(ws):
  print("WebSocket connection opened")
  
def build_stream_url():
  streams = "/".join([f"{symbol}@forceOrder" for symbol in config.SYMBOLS])
  return f"wss://fstream.binance.com/stream?streams={streams}"
  
if __name__ == "__main__":
  socket_url = build_stream_url()
  print("Connecting to WebSocket: ", socket_url)
  ws = websocket.WebSocketApp(
    socket_url,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
  )
  
  ws.on_open = on_open
  
  while True:
    try:
      ws.run_forever()
    except Exception as e:
      print("WebSocket error: ", e)
      print("Reconnecting in 5s...")
      time.sleep(5)