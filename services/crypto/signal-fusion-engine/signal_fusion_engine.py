import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.orderflow.metrics",
  "crypto.orderbook.metrics",
  "crypto.liquidation.metrics",
  "crypto.squeeze.signals",
  "crypto.microstructure.signals"
]

engine = StreamEngine(
  config.KAFKA_SERVER,
  topics,
  "signal-fusion-engine",
  window_seconds=10
)

def fuse(symbol, streams, window):
  bullish = 0
  bearish = 0
  volatility = 0
  
  if "crypto.orderflow.metrics" in streams:
      orderflow = streams["crypto.orderflow.metrics"]
      imbalance = orderflow["imbalance"]
      
      if imbalance > 0.6:
          bullish += 2
          
      if imbalance < -0.6:
          bearish += 2
          
  if "crypto.orderbook.metrics" in streams:
      orderbook = streams["crypto.orderbook.metrics"]
      imbalance = orderbook["imbalance"]
      
      if imbalance > 0.6:
          bullish += 1.5
         
      if imbalance < -0.6:
          bearish += 1.5
          
  for event in window:
      if "crypto.squeeze.signals" in event:
          squeeze = event["crypto.squeeze.signals"]
          
          if squeeze["type"] == "SHORT_SQUEEZE":
              bullish += 4
              volatility += 3
              
          if squeeze["type"] == "LONG_SQUEEZE":
              bearish += 4
              volatility += 3
              
  total = bullish + bearish
  
  if total == 0:
      return
      
  bullish_score = bullish / total
  bearish_score = bearish / total
  
  signal = create_event(
    event_type="crypto.market.score",
    source="signal-fusion-engine",
    data={
      "symbol": symbol,
      "bullish_score": bullish_score,
      "bearish_score": bearish_score,
      "volatility_score": volatility,
      "confidence": max(bullish_score, bearish_score)
    }
  )
  
  print("Market score: ", signal)
  kafka.publish("crypto.market.score", signal)
  
engine.run(fuse)