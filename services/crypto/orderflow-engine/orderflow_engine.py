import config
from datetime import datetime, timezone
from common.event import create_event
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)

buffers = {}

def process_trade(trade):
    symbol = trade["symbol"]
    
    price = float(trade["price"])
    quantity = float(trade["quantity"])
    side = trade["side"]
    
    if symbol not in buffers:
        buffers[symbol] = []
        
    buffers[symbol].append({
      "price": price,
      "quantity": quantity,
      "side": side,
    })
    
    if len(buffers[symbol]) < config.BUFFER_SIZE:
      return
    
    calculate_orderflow(symbol)
    
def calculate_orderflow(symbol):
    trades = buffers[symbol]
    
    buy_volume = 0
    sell_volume = 0
    
    for trade in trades:
        if trade["side"] == "buy":
            buy_volume += trade["quantity"]
        else:
            sell_volume += trade["quantity"]
            
    delta = buy_volume - sell_volume
    trade_count = len(trades)
    
    event = create_event(
      event_type="orderflow",
      source="orderflow-engine",
      data={
        "symbol": symbol,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "delta": delta,
        "trade_count": trade_count,
        "time": datetime.now(timezone.utc).isoformat(),
      }
    )
    print("Orderflow: ", event)
    kafka.publish("crypto.orderflow.metrics", event)    
    buffers[symbol] = []
    
for event in kafka.consume("crypto.trades", "orderflow-engine"):
    trade = event["data"]
    process_trade(trade)