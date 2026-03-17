import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.liquidation.metrics",
  "crypto.microstructure.signals",
  "crypto.orderflow.metrics",
  "crypto.trades"
]

engine = StreamEngine(
  config.KAFKA_SERVER,
  topics,
  "stop-hunt-engine",
  window_seconds=15
)

def detect(symbol, streams, window):
  signals = []
  
  signals += detect_long_trap(symbol, streams, window)
  signals += detect_short_trap(symbol, streams, window)
  
  for signal in signals:
      event = create_event(
          event_type="crypto.stop_hunt.signal",
          source="stop-hunt-engine",
          data=signal
      )
      print("Stop Hunt detected: ", event)
      kafka.publish("crypto.stop_hunt.signals", event)
      
def detect_long_trap(symbol, streams, window):
  signals = []
  liquidation_volume = 0
  sell_sweep_detected = False
  
  for event in window:
    if not isinstance(event, dict):
        continue
            
    event_type = event.get("event_type")
    if event_type == "crypto.liquidation.metrics":
      data = event.get("data", {})
      if data.get("symbol") == symbol:
         liquidation_volume += data.get("long_liquidations", 0)
      
    if event_type == "crypto.microstructure.signals":
      data = event.get("data", {})
      if data.get("symbol") != symbol:
        continue
        
      if data.get("type") == "AGGRESSIVE_SELL_SWEEP":
        sell_sweep_detected = True
        
  if (
    liquidation_volume > config.STOP_HUNT_LIQ_THRESHOLD
    and sell_sweep_detected
  ):
    signals.append({
      "symbol": symbol,
      "type": "LONG_TRAP",
      "liquidations": liquidation_volume,
      "confidence": min(liquidation_volume / (config.STOP_HUNT_LIQ_THRESHOLD * 2), 1.0)
    })
    
  return signals
  
def detect_short_trap(symbol, streams, window):
  signals = []
  liquidation_volume = 0
  buy_sweep_detected = False
  
  for event in window:
    if not isinstance(event, dict):
        continue
    event_type = event.get("event_type")
    if event_type == "crypto.liquidation.metrics":
      data = event.get("data", {})
      if data.get("symbol") == symbol:
         liquidation_volume += data.get("short_liquidations", 0)
      
    if event_type == "crypto.microstructure.signals":
      data = event.get("data", {})
      if data.get("symbol") != symbol:
        continue
        
      if data.get("type") == "AGGRESSIVE_BUY_SWEEP":
        buy_sweep_detected = True
        
  if (
    liquidation_volume > config.STOP_HUNT_LIQ_THRESHOLD
    and buy_sweep_detected
  ):
    signals.append({
      "symbol": symbol,
      "type": "SHORT_TRAP",
      "liquidations": liquidation_volume,
      "confidence": min(liquidation_volume / (config.STOP_HUNT_LIQ_THRESHOLD * 2), 1.0)
    })
    
  return signals  
  
engine.run(detect)