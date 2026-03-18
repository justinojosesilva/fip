import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.orderbook.metrics",
  "crypto.liquidation.metrics",
  "crypto.trades"
]

engine = StreamEngine(
  config.KAFKA_SERVER,
  topics,
  "liquidity-map-engine",
  window_seconds=30
)

def detect(symbol, streams, window):
  signals = []
  
  signals += detect_stop_cluster(symbol, window)
  signals += detect_liquidation_pools(symbol, window)
  signals += detect_high_liquidity(symbol, window)
  signals += detect_thin_liquidity(symbol, window)
  
  for signal in signals:
    event = create_event(
      event_type="crypto.liquidity.map",
      source="liquidity-map-engine",
      data=signal
    )
    
    print("Liquidity zone: ", event)
    kafka.publish("crypto.liquidity.map", event)
    
def detect_stop_cluster(symbol, window):
  signals = []
  price_levels = {}
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.trades":
      continue
      
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    price = round(float(data.get("price", 0)), 1)
    price_levels.setdefault(price, 0)
    price_levels[price] += 1
    
  for price, count in price_levels.items():
    if count >= config.STOP_CLUSTER_THRESHOLD:
      signals.append({
        "symbol": symbol,
        "type": "STOP_CLUSTER",
        "price": price,
        "count": count
      })
  return []

def detect_liquidation_pools(symbol, window):
  signals = []
  price_levels = {}
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.liquidation.metrics":
      continue
      
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    price = round(float(data.get("price", 0)), 1)
    size = float(data.get("size", 0))
    price_levels.setdefault(price, 0)
    price_levels[price] += size
  
  for price, total in price_levels.items():
    if total >= config.LIQ_POOL_THRESHOLD:
      signals.append({
        "symbol": symbol,
        "type": "LIQUIDATION_POOL",
        "price": price,
        "strength": total
      })
  return signals

def detect_high_liquidity(symbol, window):
  signals = []
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.orderbook.metrics":
      continue
      
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    bid_depth = data.get("bid_depth", 0)
    ask_depth = data.get("ask_depth", 0)
    mid_price = data.get("mid_price")
    
    if bid_depth >= config.HIGH_LIQ_THRESHOLD:
      signals.append({
        "symbol": symbol,
        "type": "HIGH_LIQUIDITY_ZONE",
        "side": "BID",
        "price": mid_price,
        "strength": bid_depth
      })
      
    if ask_depth >= config.HIGH_LIQ_THRESHOLD:
      signals.append({
        "symbol": symbol,
        "type": "HIGH_LIQUIDITY_ZONE",
        "side": "ASK",
        "price": mid_price,
        "strength": ask_depth
      })
      
  return signals
  
def detect_thin_liquidity(symbol, window):
  signals = []
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.orderbook.metrics":
      continue
      
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    spread = data.get("spread", 0)
    depth = data.get("depth", 0)
    
    if spread > config.THIN_SPREAD_THRESHOLD and depth < config.THIN_DEPTH_THRESHOLD:
      signals.append({
        "symbol": symbol,
        "type": "THIN_LIQUIDITY",
        "spread": spread
      })
  return signals
  
engine.run(detect)