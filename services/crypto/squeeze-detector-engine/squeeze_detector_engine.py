import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.orderflow.metrics",
  "crypto.orderbook.metrics",
  "crypto.liquidation.metrics"
]

engine = StreamEngine(
  config.KAFKA_SERVER, 
  topics, 
  "squeeze-detector-engine",
  window_seconds=10
)

def detect(symbol, streams, window):
  if "crypto.orderflow.metrics" not in streams:
      return

  if "crypto.orderbook.metrics" not in streams:
      return
  
  orderflow = streams["crypto.orderflow.metrics"]
  orderbook = streams["crypto.orderbook.metrics"]
  
  total_volume = orderflow["buy_volume"] + orderflow["sell_volume"]
  
  if total_volume == 0:
      return
  
  buy_pressure = orderflow["buy_volume"] / total_volume
  
  imbalance = orderbook["imbalance"]
  
  short_liq_window = 0
  
  for event in window:
    
      if not isinstance(event, dict):
          continue
      event_type = event.get("event_type")
      
      if event_type == "crypto.liquidation.metrics":
          data = event.get("data", {})
          short_liq_window += data.get("short_liquidations", 0)
  
  if (
    short_liq_window > config.SHORT_SQUEEZE_THRESHOLD
    and buy_pressure > config.ORDERFLOW_THRESHOLD
    and imbalance > config.ORDERBOOK_IMBALANCE_THRESHOLD
  ):
    signal = create_event(
      event_type="crypto.squeeze.signal",
      source="squeeze-detector-engine",
      data={
        "symbol": symbol,
        "type": "SHORT_SQUEEZE",
        "confidence": buy_pressure,
        "liquidations": short_liq_window
      }
    )
    
    print(f"Detected SHORT SQUEEZE: {signal}")
    kafka.publish("crypto.squeeze.signals", signal)
    
engine.run(detect)