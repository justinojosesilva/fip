import json
import time
from datetime import datetime, timezone

import config
import psycopg2
from kafka import KafkaConsumer

# kafka Consumer
def connect_kafkaConsumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "crypto.orderbook.raw",
                bootstrap_servers=config.KAFKA_SERVER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            print("Connected to Kafka")
            return consumer
        except Exception as e:
            print("Kafka not ready, retrying in 5s...")
            time.sleep(5)


consumer = connect_kafkaConsumer()


# conexão banco
def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                database=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
            )
            print("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError:
            print("Waiting for PostgreSQL...")
            time.sleep(3)


conn = connect_db()
cursor = conn.cursor()

def process_orderbook(data):
    bids = data["bids"]
    asks = data["asks"]
    
    bid_volume = 0
    ask_volume = 0
    
    for price, qty in bids[:10]:
        bid_volume += float(qty)
      
    for price, qty in asks[:10]:
        ask_volume += float(qty)
        
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    
    spread = best_ask - best_bid
    
    imbalance = bid_volume / (bid_volume + ask_volume)
    
    print(f"Processed orderbook for {symbol}: bid_volume={bid_volume}, ask_volume={ask_volume}, spread={spread}, imbalance={imbalance}")
    
    return bid_volume, ask_volume, spread, imbalance
    
def save_orderbook(symbol, bid_volume, ask_volume, spread, imbalance):
    now = datetime.now(timezone.utc)
    try:
      cursor.execute(
          """
          INSERT INTO analytics.crypto_orderbook 
          (time, symbol, bid_volume, ask_volume, spread, imbalance)
          VALUES (%s, %s, %s, %s, %s, %s)
          """,
          (now, symbol, bid_volume, ask_volume, spread, imbalance),
      )
      conn.commit()
    except Exception as e:
      print(f"Error saving orderbook data: {e}")
      conn.rollback()
      
for message in consumer:
    data = message.value
    symbol = data["symbol"]    
    bid_volume, ask_volume, spread, imbalance = process_orderbook(data)
    save_orderbook(symbol, bid_volume, ask_volume, spread, imbalance)