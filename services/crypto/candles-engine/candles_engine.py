import config
from datetime import datetime
from common.base_window_engine import BaseWindowEngine

class CandlesEngine(BaseWindowEngine):
      input_topic = "crypto.trades"
      output_topic = "crypto.candles"
      group_id = "candles-engine"
      source = "candles-engine"
      
      window_seconds = 60
      
      def process_window(self, symbol, window, events):
          if not events:
            return None
            
          # garante ordem temporal
          events.sort(key=lambda x: x["time"])
            
          prices = [float(e["price"]) for e in events]
          volumes = [float(e["quantity"]) for e in events]
          
          candle = {
            "symbol": symbol,
            "interval": "1m",
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(volumes),
            "time": datetime.utcfromtimestamp(window * self.window_seconds).isoformat()
          }
          
          return self.build_event(
            "crypto.candles",
            candle
          )
              
engine = CandlesEngine(config.KAFKA_SERVER)
        
engine.run()