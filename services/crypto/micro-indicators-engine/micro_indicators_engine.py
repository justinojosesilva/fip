import config
from datetime import datetime, timezone
from common.event import create_event
from common.stream_processor import StreamProcessor

class MicroIndicatorsEngine(StreamProcessor):
    input_topic = "crypto.trades"
    output_topic = "crypto.micro.indicators"
    group_id = "micro-indicators-engine"
    
    buffers = {}
    BUFFER_SIZE = 50
    
    symbol_state = {}
    
    def process(self, event):
        trade = event["data"]

        symbol = trade["symbol"]
        price = float(trade["price"])
        quantity = float(trade["quantity"])

        if symbol not in self.buffers:
            self.buffers[symbol] = []
        
        self.buffers[symbol].append({
          "price": price,
          "quantity": quantity
        })
        
        if len(self.buffers[symbol]) < self.BUFFER_SIZE:
            return None
        
        trades = self.buffers[symbol]
        
        prices = [t["price"] for t in trades]
        quantities = [t["quantity"] for t in trades]
        
        rsi = self.calculate_rsi(prices)
        ema = self.calculate_ema(prices)
        vwap = self.calculate_vwap(prices, quantities)

        event = create_event(
            event_type="indicators",
            source="indicators-engine",
            data={
              "symbol": symbol,
              "price": price,
              "ema": ema,
              "rsi": rsi,
              "vwap": vwap,
              "volume": sum(quantities),
              "time": datetime.now(timezone.utc).isoformat(),
            }
        )
        print("Indicators: ", event)
        return event
    
    def calculate_ema(self, prices, period=config.EMA_PERIOD):
        if len(prices) < period:
            return None
        multiplier = 2 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = (price - ema) * multiplier + ema
        return ema
    
    def calculate_rsi(self, prices, period=config.RSI_PERIOD):
        if len(prices) < period + 1:
            return None
            
        gains = []
        losses = []
        
        for i in range(1, period + 1):
          
            diff = prices[i] - prices[i - 1]
            
            if diff >= 0:
              gains.append(diff)
            else:
              losses.append(abs(diff))
              
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        
        if avg_loss == 0:
            return 100
            
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
        
    def calculate_vwap(self, prices, quantities):
        total_volume = sum(quantities)
        if total_volume == 0:
          return None
          
        vwap = sum(p * q for p, q in zip(prices, quantities)) / total_volume
        return vwap
        
engine = MicroIndicatorsEngine(config.KAFKA_SERVER)

engine.run()
