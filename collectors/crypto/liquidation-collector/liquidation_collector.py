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
  
    if "data" not in data:
        return
  
    payload = data["data"]
  
    if "o" not in payload:
        return
  
    order = payload["o"]
  
    symbol = order["s"]
    side = order["S"]
    quantity = float(order["q"])
    price = float(order["ap"])
  
    timestamp = payload["E"]
    
    event_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    
    liquidation_event = create_event(
        event_type="liquidations",
        source="liquidation-collector",
        data={
            "symbol": symbol,
            "side": side,
            "price": price,
            "quantity": quantity,
            "time": event_time.isoformat(),
            "exchange": config.EXCHANGE
        }
    )
    print("Liquidation:", liquidation_event)
    kafka.publish("crypto.liquidations", liquidation_event)     
    
def build_stream_url():
  streams = "/".join([f"{symbol.lower()}@forceOrder" for symbol in config.SYMBOLS])
  return f"wss://fstream.binance.com/stream?streams={streams}"
 
socket_url = build_stream_url()

ws = WebSocketClient(socket_url, handle_message)

ws.run()