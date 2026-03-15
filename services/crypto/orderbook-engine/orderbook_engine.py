import json
import time
import config
from datetime import datetime, timezone
from common.event import create_event
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)

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
    

      
for event in kafka.consume("crypto.orderbook.raw","orderbook-engine"):
    data = event["data"]
    symbol = data["symbol"]
    
    result = process_orderbook(data)

    if result is None:
        continue

    bid_volume, ask_volume, spread, imbalance = result

    event = create_event(
        event_type="orderbook",
        source="orderbook-engine",
        data={
            "symbol": symbol,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume,
            "spread": spread,
            "imbalance": imbalance,
            "time": datetime.now(timezone.utc).isoformat()
        }
    )
    print("Orderbook metrics:", event)
    kafka.publish("crypto.orderbook.metrics", event)

    