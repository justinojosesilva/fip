import json
import time
import config

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
from common.event import create_event

buffers = {}

# kafka
def connect_kafka():
    while True:
      try:
          consumer = KafkaConsumer(
              "crypto.trades",
              bootstrap_servers=config.KAFKA_SERVER,
              value_deserializer=lambda x: json.loads(x.decode("utf-8")),
              auto_offset_reset="earliest",
              enable_auto_commit=True,
          )
          producer = KafkaProducer(
              bootstrap_servers=config.KAFKA_SERVER,
              value_serializer=lambda x: json.dumps(x).encode("utf-8"),
          )
          print("Connected to Kafka")
          return consumer, producer
      except Exception as e:
          print("Kafka not ready, retrying in 5s...")
          time.sleep(5)


consumer, producer = connect_kafka() 

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
    producer.send("crypto.orderflow.metrics", event)    
    buffers[symbol] = []
    
for message in consumer:
    event = message.value
    trade = event["data"]
    process_trade(trade)