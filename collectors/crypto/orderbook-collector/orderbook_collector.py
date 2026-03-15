import json
import websocket
import config
import time
from kafka import KafkaProducer
from common.event import create_event

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
  bids = data["b"]
  asks = data["a"]
  timestamp = data["E"]

  orderbook_data = create_event(
    event_type="orderbook",
    source="orderbook-collector",
    data={
      "symbol": symbol,
      "bids": bids,
      "asks": asks,
      "timestamp": timestamp,
      "exchange": config.EXCHANGE
    }
  )

  producer.send("crypto.orderbook.raw", orderbook_data)
  
  print(orderbook_data)
  
def on_error(ws, error):
  print("WebSocket error: ", error)
  
def on_close(ws, close_status_code, close_msg):
  print("WebSocket closed: ", close_status_code, close_msg)
  
def on_open(ws):
  print("WebSocket connection opened")
  
if __name__ == "__main__":
  ws = websocket.WebSocketApp(
    config.WS_URL,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
  )

  ws.on_open = on_open
  ws.run_forever()