import json
import config
from common.event import create_event
from common.base_kafka import BaseKafka
from common.websocket_client import WebSocketClient

kafka = BaseKafka(config.KAFKA_SERVER)

def handle_message(ws, message):
  try:
    data = json.loads(message)
  except Exception:
    return
  
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
  print(orderbook_data)
  kafka.publish("crypto.orderbook.raw", orderbook_data)
  
ws = WebSocketClient(config.WS_URL, handle_message)

ws.run()