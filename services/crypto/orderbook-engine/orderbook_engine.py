import json
import time
from datetime import datetime, timezone

import config
from kafka import KafkaConsumer, KafkaProducer

# kafka Consumer
def connect_kafkaConsumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "crypto.orderbook.raw",
                bootstrap_servers=config.KAFKA_SERVER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka")
            return consumer, producer
        except Exception as e:
            print("Kafka not ready, retrying in 5s...")
            time.sleep(5)

consumer, producer = connect_kafkaConsumer()

def process_orderbook(data):
    bids = data.get("bids",[])
    asks = data.get("asks",[])
    
    if not bids or not asks:
        print("Empty orderbook received")
        return None
    
    bid_volume = 0
    ask_volume = 0
    
    for price, qty in bids[:10]:
        bid_volume += float(qty)
      
    for price, qty in asks[:10]:
        ask_volume += float(qty)
        
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    
    spread = best_ask - best_bid
    
    total_volume = bid_volume + ask_volume
    
    if total_volume == 0:
        imbalance = 0
    else:    
      imbalance = bid_volume / total_volume
      
    return bid_volume, ask_volume, spread, imbalance
    

      
for message in consumer:
    data = message.value 
    symbol = data["symbol"]
    
    result = process_orderbook(data)

    if result is None:
        continue

    bid_volume, ask_volume, spread, imbalance = result

    event = {
        "symbol": symbol,
        "bid_volume": bid_volume,
        "ask_volume": ask_volume,
        "spread": spread,
        "imbalance": imbalance,
        "time": datetime.now(timezone.utc).isoformat()
    }

    producer.send("crypto.orderbook.metrics", event)

    print("Orderbook metrics:", event)