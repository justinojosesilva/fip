import json
import time
import config

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone

# kafka
def connect_kafka():
  while True:
    try:
      consumer = KafkaConsumer(
        'crypto.trades',
        bootstrap_servers=config.KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset="latest",
      )
      producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
      )
      print("Connected to Kafka")
      return consumer, producer
    except Exception as e:
      print(f"Error connecting to Kafka: {e}")
      time.sleep(5)
    
consumer, producer = connect_kafka()

def save_whale(symbol, price, quantity, side, trade_value):
    now = datetime.now(timezone.utc)
    try:
      cursor.execute("""
        INSERT INTO analytics.crypto_whales 
        (time, symbol, price, quantity, trade_value, side)
        VALUES (%s, %s, %s, %s, %s, %s)
      """, (now, symbol, price, quantity, trade_value, side))
      conn.commit()
    except Exception as e:
      print(f"Error saving whale to database: {e}")
      conn.rollback()
      
def process_trade(trade):
    symbol = trade['symbol']
    price = float(trade['price'])
    quantity = float(trade['quantity'])
    side = trade['side']
    
    trade_value = price * quantity
    
    if trade_value >= config.WHALE_THRESHOLD:
      print(f"WHALE DETECTED: {trade}")
      
      whale_event = {
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "value": trade_value,
        "side": side,
        "time": datetime.now(timezone.utc).isoformat()
      }
      
      producer.send("crypto.whales.detected", whale_event)
      
      print("WHALE DETECTED: ", whale_event)
      
      
      
# loop principal
for message in consumer:
    trade = message.value
    process_trade(trade)