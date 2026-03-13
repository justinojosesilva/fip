import json
import time
import psycopg2
import config

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone

# conexão banco
def connect_db():
  while True:
    try:
      conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        database=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD
      )
      return conn
    except psycopg2.OperationalError as e:
      print(f"Error connecting to PostgreSQL: {e}")
      time.sleep(5)
      
conn = connect_db()
cursor = conn.cursor()

# kafka Consumer
def connect_kafkaConsumer():
  while True:
    try:
      consumer = KafkaConsumer(
        'crypto.trades',
        bootstrap_servers=config.KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
      )
      print("Connected to Kafka")
      return consumer
    except Exception as e:
      print(f"Error connecting to Kafka: {e}")
      time.sleep(5)
    
consumer = connect_kafkaConsumer()

# kafka producer
def connect_kafkaProducer():
  while True:
    try:
      producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
      )
      print("Connected to Kafka")
      return producer
    except Exception as e:
      print("Kafka not ready, retrying in 5s...")
      time.sleep(5)

producer = connect_kafkaProducer()

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
      }
      
      save_whale(symbol, price, quantity, side, trade_value)
      
      producer.send("crypto.whales", whale_event)
      
      
      
# loop principal
for message in consumer:
    trade = message.value
    process_trade(trade)