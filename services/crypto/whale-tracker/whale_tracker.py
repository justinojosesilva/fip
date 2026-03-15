import json
import time
import config

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
from common.event import create_event
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)


def process_trade(trade):
    symbol = trade['symbol']
    price = float(trade['price'])
    quantity = float(trade['quantity'])
    side = trade['side']
    
    trade_value = price * quantity
    
    if trade_value >= config.WHALE_THRESHOLD:
      print(f"WHALE DETECTED: {trade}")
      
      whale_event = create_event(
        event_type="whale_detected",
        source="whale_tracker",
        data={
          "symbol": symbol,
          "price": price,
          "quantity": quantity,
          "value": trade_value,
          "side": side,
          "time": datetime.now(timezone.utc).isoformat()
        }
      )
      print("WHALE DETECTED: ", whale_event)
      kafka.publish("crypto.whales.detected", whale_event)
      
      
# loop principal
for event in kafka.consume("crypto.trades", "whale-tracker"):
    trade = event["data"]
    process_trade(trade)