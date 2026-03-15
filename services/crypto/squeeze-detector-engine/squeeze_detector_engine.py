from this import s
import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.orderflow",
  "crypto.orderbook.raw",
  "crypto.liquidation.metrics"
]

engine = StreamEngine(
  config.KAFKA_SERVER, 
  topics, 
  "squeeze-detector-engine",
  window_seconds=10
)

def detect(symbol, streams, window):
  orderflow = streams["crypto.orderflow"]
  orderbook = streams["crypto.orderbook.raw"]
  
  buy_pressure = orderflow["buy_ratio"]
  
  imbalance = orderbook["imbalance"]
  
  short_liq_window = 0
  
  for event in window:
    
    if "crypto.liquidation.metrics" in event:
      short_liq_window += event["crypto.liquidation.metrics"]["short_liquidations"]
  
  if (
    short_liq_window > config.SHORT_SQUEEZE_THRESHOLD
    and buy_pressure > config.ORDERFLOW_THRESHOLD
    and imbalance > config.ORDERBOOK_IMBALANCE_THRESHOLD
  ):
    signal = create_event(
      event_type="squeeze_signal",
      source="squeeze_detector_engine",
      data={
        "symbol": symbol,
        "type": "LONG_SQUEEZE",
        "confidence": buy_pressure,
      }
    )
    
    print(f"Detected LONG SQUEEZE: {signal}")
    kafka.publish("crypto.squeeze.signals", signal)
    
engine.run(detect)