import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.microstructure.signals",
  "crypto.squeeze.signals",
  "crypto.stop_hunt.signals",
  "crypto.orderflow.metrics",
  "crypto.liquidation.metrics",
  "crypto.market.regime",
  "crypto.liquidity.map"
]

engine = StreamEngine(
  config.KAFKA_SERVER,
  topics,
  "opportunity-engine",
  window_seconds=20
)



def detect(symbol, streams, window):
  
  regime, confidence = get_market_regime(symbol, window)
  
  if regime == "LOW_LIQUIDITY":
    return
  
  signals = []
  signals += detect_reversal(symbol, streams, window)
  signals += detect_breakout(symbol, streams, window)
  signals += detect_squeeze_setup(symbol, streams, window)
  
  for signal in signals:
    event = create_event(
      event_type="crypto.opportunity.signal",
      source="opportunity-engine",
      data=signal
    )
    
    print("Opportunity detected:", event)
    kafka.publish("crypto.opportunity.signals", event)
    
  score = compute_score(symbol, window, regime)
  
  threshold = config.OPPORTUNITY_SCORE_THRESHOLD
   
  if regime == "VOLATILE":
    threshold *= 2  # Increase threshold in volatile markets
   
  if score < threshold:
     return
     
  direction = infer_direction(symbol, window)
  
  opportunity = {
    "symbol": symbol,
    "type": "HIGH_PROBABILITY_SETUP",
    "score": score,
    "direction": direction,
    "regime": regime,
    "regime_confidence": confidence
  }
  
  event = create_event(
    event_type="crypto.opportunity.signal",
    source="opportunity-engine",
    data=opportunity
  )
  
  print("High probability opportunity detected:", event)
  kafka.publish("crypto.opportunity.signals", event)
  
def compute_score(symbol, window, regime):
  score = 0
  seen = set()

  liquidity_zones = get_liquidity_zones(symbol, window)
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    event_type = event.get("event_type")
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    if event_type == "crypto.stop_hunt.signals" and "stop_hunt" not in seen:
      if regime == "RANGE":
        score += config.SCORE_STOP_HUNT * 1.5
      else:
        score += config.SCORE_STOP_HUNT
      seen.add("stop_hunt")
      
    if event_type == "crypto.microstructure.signals":
      signal_type = data.get("type", "")
      
      if signal_type == "ABSORPTION" and "absorption" not in seen:
        if regime == "RANGE":
          score += config.SCORE_ABSORPTION * 1.5
        else: 
          score += config.SCORE_ABSORPTION
        seen.add("absorption")
        
      if "SWEEP" in signal_type and "sweep" not in seen:
        
        if regime == "TREND":
          score += config.SCORE_SWEEP * 1.5
        else:
          score += config.SCORE_SWEEP
        seen.add("sweep")
        
      if signal_type == "LIQUIDITY_VACUUM" and "vacuum" not in seen:
        
        if regime == "TREND":
          score += config.SCORE_LIQUIDITY_VACUUM * 1.5
        else:
          score += config.SCORE_LIQUIDITY_VACUUM
        seen.add("vacuum")
        
    if event_type == "crypto.squeeze.signals" and "squeeze" not in seen:
      score += config.SCORE_SQUEEZE
      seen.add("squeeze")
      
    if event_type == "crypto.orderflow.metrics" and "orderflow" not in seen:
      buy_ratio = data.get("buy_ratio", 0)
      
      if buy_ratio > 0.7:
        score += config.SCORE_ORDERFLOW_IMBALANCE
      if buy_ratio < 0.3:
        score += config.SCORE_ORDERFLOW_IMBALANCE
      seen.add("orderflow")
        
    if event_type == "crypto.liquidation.metrics" and "liquidation" not in seen:
      long_liq = data.get("long_liquidations", 0)
      short_liq = data.get("short_liquidations", 0)
      if long_liq > config.LIQ_SPIKE:
        score += config.SCORE_LIQUIDATION_SPIKE
        
      if short_liq > config.LIQ_SPIKE:
        score += config.SCORE_LIQUIDATION_SPIKE
        
      seen.add("liquidation")
        
  for zone in liquidity_zones:
    zone_type = zone.get("type")
    
    if zone_type == "LIQUIDATION_POOL":
      score += config.SCORE_LIQUIDITY_POOL
      
    if zone_type == "STOP_CLUSTER":
      score += config.SCORE_STOP_CLUSTER
      
    if zone_type == "THIN_LIQUIDITY":
      if regime == "TREND":
        score += config.SCORE_THIN_LIQUIDITY * 1.5
      else:
        score += config.SCORE_THIN_LIQUIDITY
        
    if zone_type == "HIGH_LIQUIDITY_ZONE":
      if regime == "RANGE":
        score += config.SCORE_HIGH_LIQUIDITY

  return score
  
def infer_direction(symbol, window):
  
  buy_pressure = 0
  sell_pressure = 0
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") == "crypto.orderflow.metrics":
      data = event.get("data", {})
      
      if data.get("symbol") != symbol:
        continue
        
      ratio = data.get("buy_ratio", 0.5)
      
      if ratio > 0.6:
        buy_pressure += 1
        
      if ratio < 0.4:
        sell_pressure += 1
        
    if event.get("event_type") == "crypto.squeeze.signals":
      data = event.get("data", {})
      
      if data.get("symbol") != symbol:
        continue
        
      if  data.get("type") == "SHORT_SQUEEZE_SETUP":
        buy_pressure += 2
  
  if buy_pressure > sell_pressure:
    return "LONG"
    
  return "SHORT"
    
def detect_reversal(symbol, streams, window):
  signals = []
  
  stop_hunt = None
  absorption = None
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    event_type = event.get("event_type")
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    if event_type == "crypto.stop_hunt.signals":
      stop_hunt = data
      
    if event_type == "crypto.microstructure.signals":
      if data.get("type") == "ABSORPTION":
        absorption = True
  
  if stop_hunt and absorption:
    signals.append({
      "symbol": symbol,
      "type": "REVERSAL_SETUP",
      "direction": "LONG" if stop_hunt.get("type") == "LONG_TRAP" else "SHORT",
      "confidence": config.REVERSAL_CONFIDENCE,
      "details": {
        "stop_hunt": stop_hunt,
        "absorption": absorption
      }
    })
  return signals
  
def detect_breakout(symbol, streams, window):
  signals = []
  
  sweep = False
  vacuum = False
  
  for event in window:
    
    if not isinstance(event, dict):
      continue
      
    event_type = event.get("event_type")
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    if event_type == "crypto.microstructure.signals":
      if "SWEEP" in data.get("type", ""):
        sweep = True
        
      if data.get("type") == "LIQUIDITY_VACUUM":
        vacuum = True
        
  if sweep and vacuum:
    signals.append({
      "symbol": symbol,
      "type": "BREAKOUT_SETUP",
      "confidence": config.BREAKOUT_CONFIDENCE,
      "details": {
        "sweep": sweep,
        "vacuum": vacuum
      }
    })
  return signals
  
def detect_squeeze_setup(symbol, streams, window):
  signals = []
  
  squeeze = None
  buy_pressure = None
  
  for event in window:
    
    if not isinstance(event, dict):
      continue
      
    event_type = event.get("event_type")
    data = event.get("data", {})
    
    if data.get("symbol") != symbol:
      continue
      
    if event_type == "crypto.squeeze.signals":
      squeeze = data
      
    if event_type == "crypto.orderflow.metrics":
      if data.get("buy_ratio", 0) > 0.7:
        buy_pressure = True
        
  if squeeze and buy_pressure:
    signals.append({
      "symbol": symbol,
      "type": "SHORT_SQUEEZE_SETUP",
      "direction": "LONG",
      "confidence": config.SQUEEZE_CONFIDENCE,
      "details": {
        "squeeze": squeeze,
        "buy_pressure": buy_pressure
      }
    })
  return signals
  
def get_market_regime(symbol, window):
  regime = None
  confidence = 0
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.market.regime":
      continue
    
    data = event.get("data", {})
      
    if data.get("symbol") != symbol:
      continue
      
    regime = data.get("regime")
    confidence = data.get("confidence", 0)
    
  return regime, confidence
  
def get_liquidity_zones(symbol, window):
  zones = []
  
  for event in window:
    if not isinstance(event, dict):
      continue
      
    if event.get("event_type") != "crypto.liquidity.map":
      continue
    
    data = event.get("data", {})
      
    if data.get("symbol") != symbol:
      continue
      
    zones.append(data)
    
  return zones
  
engine.run(detect)