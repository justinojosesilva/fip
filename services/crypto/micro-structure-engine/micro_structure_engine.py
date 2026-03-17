import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.orderflow.metrics",
  "crypto.orderbook.metrics",
  "crypto.trades"
]

engine = StreamEngine(
  config.KAFKA_SERVER,
  topics,
  "microstructure-engine",
  window_seconds=5
)

def detect(symbol, streams, window):
    if "crypto.orderflow.metrics" not in streams:
      return
    
    if "crypto.orderbook.metrics" not in streams:
      return
      
    orderflow = streams["crypto.orderflow.metrics"]
    orderbook = streams["crypto.orderbook.metrics"]
    
    signals = []
    
    signals += detect_absorption(symbol, orderflow, orderbook)
    signals += detect_liquidity_vacuum(symbol, orderflow, orderbook)
    signals += detect_iceberg(symbol, window)
    
    for signal in signals:
        event = create_event(
          event_type="crypto.microstructure.signal",
          source="microstructure-engine",
          data=signal
        )
        
        print("Microstructure signal: ", event)
        kafka.publish("crypto.microstructure.signals", event)
        
def detect_absorption(symbol, orderflow, orderbook):
    
    signals = []
    
    if (
      abs(orderflow["delta"]) > config.ABSORPTION_DELTA_THRESHOLD
      and abs(orderbook["imbalance"]) < config.ABSORPTION_IMBALANCE_THRESHOLD
    ):
      signals.append({
        "symbol": symbol,
        "type": "ABSORPTION",
        "delta": orderflow["delta"],
        "imbalance": orderbook["imbalance"]
      })
      
    return signals
    
def detect_liquidity_vacuum(symbol, orderflow, orderbook):

    signals = []

    if (
      orderbook["spread"] > config.VACUUM_SPREAD_THRESHOLD
      and abs(orderflow["imbalance"]) > config.VACUUM_ORDERFLOW_THRESHOLD
    ):

      signals.append({
          "symbol": symbol,
          "type": "LIQUIDITY_VACUUM",
          "spread": orderbook["spread"],
          "orderflow": orderflow["imbalance"]
      })

    return signals
    
def detect_iceberg(symbol, window):

    signals = []
    trade_sizes = []

    for event in window:
        if "crypto.trades" in event:
            trade = event["crypto.trades"]
            trade_sizes.append(float(trade["quantity"]))

    if len(trade_sizes) < 10:
        return signals
        
    avg_size = sum(trade_sizes) / len(trade_sizes)
    large_trades = [t for t in trade_sizes if t > avg_size * 5]

    if len(large_trades) > config.ICEBERG_TRADE_THRESHOLD:
        signals.append({
            "symbol": symbol,
            "type": "ICEBERG_ORDER",
            "large_trades": len(large_trades),
            "avg_trade_size": avg_size
        })

    return signals
    
engine.run(detect)