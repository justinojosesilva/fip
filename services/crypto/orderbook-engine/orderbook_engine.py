import config
from datetime import datetime, timezone
from common.base_engine import BaseEngine

class OrderBookEngine(BaseEngine):
  
    input_topic = "crypto.orderbook.raw"
    output_topic = "crypto.orderbook.metrics"
    group_id = "orderbook-engine"
    source = "orderbook-engine"

    DEPTH = 10
    
    def process(self, event):
        
        data = event["data"]
        symbol = data["symbol"]
        bids = data.get("bids",[])
        asks = data.get("asks",[])
        
        if not bids or not asks:
            print("Empty orderbook received")
            return None
            
        bid_volume = sum(float(qty) for _, qty in bids[:self.DEPTH])
        ask_volume = sum(float(qty) for _, qty in asks[:self.DEPTH])
        
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        
        spread = best_ask - best_bid
        
        total_volume = bid_volume + ask_volume
        
        imbalance = 0 if total_volume == 0 else bid_volume / total_volume
        
        metric = {
          "symbol": symbol,
          "bid_volume": bid_volume,
          "ask_volume": ask_volume,
          "spread": spread,
          "imbalance": imbalance,
          "depth": self.DEPTH,
          "time": datetime.now(timezone.utc).isoformat()
        }
        
        return self.build_event(
          "crypto.orderbook.metrics",
          metric
        )
        
engine = OrderBookEngine(config.KAFKA_SERVER)

engine.run()