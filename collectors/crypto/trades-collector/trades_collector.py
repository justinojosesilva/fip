import json
import config
from datetime import datetime, timezone
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
  trade_id = data["t"]
  
  if "p" not in data:
    return

  price = float(data["p"])
  quantity = float(data["q"])
  
  timestamp = data["T"]
  is_maker = data["m"]
  
  trade_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
  
  side = "sell" if is_maker else "buy"

  event = create_event(
    event_type="crypto.trades",
    source="trades-collector",
    data={
      "symbol": symbol,
      "price": price,
      "quantity": quantity,
      "side": side,
      "trade_id": trade_id,
      "time": trade_time.isoformat(),
      "exchange": config.EXCHANGE
    }
  )
  
  event_price = create_event(
    event_type="prices",
    source="trades-collector",
    data={
      "symbol": symbol,
      "price": price,
      "time": trade_time.isoformat(),
      "exchange": config.EXCHANGE
    }
  )

  # envia para kafka
  print("Trade event:", event)
  kafka.publish("crypto.trades", event)
  print("Price event:", event_price)
  kafka.publish("crypto.prices", event_price)
  
socket = f"wss://stream.binance.com:9443/ws/{config.SYMBOL}@trade"

ws = WebSocketClient(socket, handle_message)

ws.run()