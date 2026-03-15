import time
import config
from datetime import datetime
from common.event import create_event
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)

windows = {}

def process_liquidation(event):
    symbol = event["symbol"]
    side = event["side"]
    quantity = float(event["quantity"])
    price = float(event["price"])
    
    value = quantity * price
    
    event_time = datetime.fromisoformat(event["time"]).timestamp()
    
    window = int(event_time // config.WINDOW_SECONDS)
    
    key = f"{symbol}-{window}"
    
    if key not in windows:
        windows[key] = {
            "symbol": symbol,
            "long_liquidations": 0,
            "short_liquidations": 0
        }
    
    if side == "SELL":
        windows[key]["long_liquidations"] += value
    else:
        windows[key]["short_liquidations"] += value
        
def detect_cascade(event):
    long_liq = event["long_liquidations"]
    short_liq = event["short_liquidations"]
    
    if long_liq > config.CASCADE_THRESHOLD:
       cascade = {
         "symbol": event["symbol"],
         "type": "LONG_CASCADE",
         "volume": long_liq,
         "time": event["time"]
       }
       print("LONG CASCADE DETECTED: ", cascade)
       kafka.publish("crypto.liquidation.cascades", cascade)
    if short_liq > config.CASCADE_THRESHOLD:
       cascade = {
         "symbol": event["symbol"],
         "type": "SHORT_CASCADE",
         "volume": short_liq,
         "time": event["time"]
       }
       print("SHORT CASCADE DETECTED: ", cascade)
       kafka.publish("crypto.liquidation.cascades", cascade)
        
def flush_window():
    now_window = int(time.time() // config.WINDOW_SECONDS)
    keys_to_delete = []
    for key in windows:
        symbol, window = key.split("-")
        window = int(window)
        if window < now_window:
          data = windows[key]
          event = create_event(
            event_type="liquidation",
            source="liquidation-engine",
            data = {
              "symbol": symbol,
              "interval": "1m",
              "long_liquidations": data["long_liquidations"],
              "short_liquidations": data["short_liquidations"],
              "liquidation_imbalance": data["long_liquidations"] - data["short_liquidations"],
              "time": datetime.utcfromtimestamp(window * config.WINDOW_SECONDS).isoformat()
            } 
          )
          
          print("Liquidation metric: ", event)
          kafka.publish("crypto.liquidation.metrics", event)
          detect_cascade(event)
          keys_to_delete.append(key)
          
    for key in keys_to_delete:
        del windows[key]
        
for event in kafka.consume("crypto.liquidations","liquidation-engine"):
    data = event["data"]
    process_liquidation(data)
    flush_window()