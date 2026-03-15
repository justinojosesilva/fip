import json
import websocket
import time
import config
from datetime import datetime, timezone
from common.event import create_event
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)

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
  

def on_error(ws, error):
  print(error)

def on_close(ws, close_status_code, close_msg):
  print("connection closed:", close_status_code, close_msg)

def on_open(ws):
  print("connected to Binance websocket")

if __name__ == "__main__":
  socket = f"wss://stream.binance.com:9443/ws/{config.SYMBOL}@trade"
  while True:
    try:
      ws = websocket.WebSocketApp(
        socket,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
      )
    
      ws.on_open = on_open
      ws.run_forever()
    except Exception as e:
      print(f"Websocket error: {e}, reconnecting in 5s...")
      
    print("Reconnecting in 5 seconds...")
    time.sleep(5)
  