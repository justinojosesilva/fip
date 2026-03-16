import config

from datetime import datetime, timezone
from common.base_engine import BaseEngine

class WhaleTrackerEngine(BaseEngine):
    input_topic = "crypto.trades"
    output_topic = "crypto.whales.detected"
    group_id = "whale-tracker"
    source = "whale-tracker"
    
    def process(self, event):
        trade = event["data"]
        
        symbol = trade['symbol']
        price = float(trade['price'])
        quantity = float(trade['quantity'])
        side = trade['side']
        
        trade_value = price * quantity
        
        if trade_value < config.WHALE_THRESHOLD:
            return None
            
        if trade_value > 5 * config.WHALE_THRESHOLD:
            size = "mega"
        elif trade_value > 2 * config.WHALE_THRESHOLD:
            size = "large"
        else:
            size = "normal"
            
        impact = trade_value / config.WHALE_THRESHOLD
        
        whale_event = {
          "symbol": symbol,
          "price": price,
          "quantity": quantity,
          "value": trade_value,
          "side": side,
          "impact": impact,
          "size": size,
          "time": datetime.now(timezone.utc).isoformat()
        }
        print("WHALE DETECTED: ", whale_event)
        return self.build_event(
          "crypto.whales.detected",
          whale_event
        )
        
engine = WhaleTrackerEngine(config.KAFKA_SERVER)

engine.run()