import config
from datetime import datetime
from common.base_window_engine import BaseWindowEngine

class LiquidationEngine(BaseWindowEngine):
      input_topic = "crypto.liquidations"
      output_topic = "crypto.liquidation.metrics"
      group_id = "liquidation-engine"
      source = "liquidation-engine"
  
      window_seconds = 60
  
      def process_window(self, symbol, window, events):
  
          long_liq = 0
          short_liq = 0
  
          for e in events:
  
              value = float(e["price"]) * float(e["quantity"])
  
              if e["side"] == "SELL":
                  long_liq += value
              else:
                  short_liq += value
  
          metric = {
              "symbol": symbol,
              "interval": "1m",
              "long_liquidations": long_liq,
              "short_liquidations": short_liq,
              "liquidation_imbalance": long_liq - short_liq,
              "time": datetime.utcfromtimestamp(
                  window * self.window_seconds
              ).isoformat()
          }
  
          metric_event = self.build_event(
              "crypto.liquidation.metrics",
              metric
          )
          
          # Detect cascade
          cascades = self.detect_cascade(metric)
          results = [metric_event]
          
          if cascades:
            results.extend(cascades)
            
          return results
          
      def detect_cascade(self, metric):
          cascades = []
          long_liq = metric["long_liquidations"]
          short_liq = metric["short_liquidations"]
          
          if long_liq > config.CASCADE_THRESHOLD:
            cascades = {
              "symbol": metric["symbol"],
              "type": "LONG_CASCADE",
              "volume": long_liq,
              "time": metric["time"]
            }
            
            event = self.buld_event(
              "crypto.liquidation.cascades",
              cascades
            )
            
            self.cascades.append(event)
          
          if short_liq > config.CASCADE_THRESHOLD:
            cascades = {
              "symbol": metric["symbol"],
              "type": "SHORT_CASCADE",
              "volume": short_liq,
              "time": metric["time"]
            }
            
            event = self.build_event(
              "crypto.liquidation.cascades",
              cascades
            )
            
            self.cascades.append(event)
            
          return cascades

engine = LiquidationEngine(config.KAFKA_SERVER)

engine.run()