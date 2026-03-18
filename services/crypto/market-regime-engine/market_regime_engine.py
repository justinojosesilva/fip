import re
import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.orderflow.metrics",
  "crypto.orderbook.metrics",
  "crypto.liquidation.metrics",
  "crypto.trades"
]

engine = StreamEngine(
  config.KAFKA_SERVER,
  topics,
  "market-regime-engine",
  window_seconds=20
)

def detect(symbol, streams, window):
  regime = detect_regime(symbol, window)
  
  if regime is None:
    return
    
  event = create_event(
    event_type="crypto.market.regime",
    source="market-regime-engine",
    data=regime
  )
  
  print(f"Detected regime for {symbol}: {regime}")
  kafka.publish("crypto.market.regime", event)
  
def detect_regime(symbol, window):
  volatility = compute_volatility(symbol, window)
  liquidity = compute_liquidity(symbol, window)
  directionality = compute_directionality(symbol, window)
  
  #VOLATILE
  if volatility > config.REGIME_VOLATILITY_HIGH:
    return {
      "symbol": symbol,
      "regime": "VOLATILE",
      "confidence": volatility
    }
  
  # LOW LIQUIDITY
  if liquidity < config.REGIME_LIQUIDITY_LOW:
    return {
      "symbol": symbol,
      "regime": "LOW_LIQUIDITY",
      "confidence": 1 - liquidity
    }
    
  # TREND
  if directionality > config.REGIME_TREND_THRESHOLD:
    return {
      "symbol": symbol,
      "regime": "TREND",
      "confidence": directionality
    }
    
  # RANGE
  return {
    "symbol": symbol,
    "regime": "RANGE",
    "confidence": 0.6
  }
  
def compute_volatility(symbol, window):
  prices = []
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("symbol") != symbol:
      continue
      
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    price = float(data.get("price", 0))
    if price > 0:
      prices.append(price)
      
  if len(prices) < 5:
    return 0
    
  mean_price = sum(prices) / len(prices)
  
  variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
  
  volatility = (variance ** 0.5) / mean_price
  
  return volatility 
  
def compute_liquidity(symbol, window):
  depth_values = []
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.orderbook.metrics":
      continue
    
    data = event.get("data", {})
      
    if data.get("symbol") != symbol:
      continue
      
    depth = data.get("depth", 0)
    
    depth_values.append(depth)
    
  if not depth_values:
    return 0
    
  avg_depth = sum(depth_values) / len(depth_values)
  
  return avg_depth
  
def compute_directionality(symbol, window):
  buy_pressure = 0
  sell_pressure = 0
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.orderflow.metrics":
      continue
    
    data = event.get("data", {})
      
    if data.get("symbol") != symbol:
      continue
      
    ratio = data.get("buy_ratio", 0.5)
    
    if ratio > 0.6:
      buy_pressure += 1
      
    if ratio < 0.4:
      sell_pressure += 1
      
  total = buy_pressure + sell_pressure
  
  if total == 0:
    return 0
    
  directionality = abs(buy_pressure - sell_pressure) / total
  
  return directionality
  
engine.run(detect)