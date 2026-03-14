import json
import time
import psycopg2
import config

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone

buffers = {}

def connect_db():
    while True:
      try:
        conn = psycopg2.connect(
          host=config.POSTGRES_HOST,
          database=config.POSTGRES_DB,
          user=config.POSTGRES_USER,
          password=config.POSTGRES_PASSWORD
        )
        
        print("Connected to PostgreSQL")
        return conn
      except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        time.sleep(5)
      
conn = connect_db()
cursor = conn.cursor()

# kafka Consumer
def connect_kafkaConsumer():
    while True:
      try:
          consumer = KafkaConsumer(
              "crypto.trades",
              bootstrap_servers=config.KAFKA_SERVER,
              value_deserializer=lambda x: json.loads(x.decode("utf-8")),
          )
          print("Connected to Kafka")
          return consumer
      except Exception as e:
          print("Kafka not ready, retrying in 5s...")
          time.sleep(5)


consumer = connect_kafkaConsumer() 
      
# kafka Producer
def connect_kafkaProducer():
    while True:
      try:
          producer = KafkaProducer(
              bootstrap_servers=config.KAFKA_SERVER,
              value_serializer=lambda x: json.dumps(x).encode("utf-8"),
          )
          print("Connected to Kafka Producer")
          return producer
      except Exception as e:
          print("Kafka Producer not ready, retrying in 5s...")
          time.sleep(5)
      
producer = connect_kafkaProducer()

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
    save_orderflow(symbol, buy_volume, sell_volume, delta, trade_count)
    buffers[symbol] = []
    
def save_orderflow(symbol, buy_volume, sell_volume, delta, trade_count):
    now = datetime.now(timezone.utc)
    
    cursor.execute("""
      INSERT INTO analytics.crypto_orderflow(time, symbol, buy_volume, sell_volume, delta, trade_count) 
      VALUES (%s, %s, %s, %s, %s, %s)
        """, (now, symbol, buy_volume, sell_volume, delta, trade_count)
    )
    conn.commit()
    
    event = {
      "symbol": symbol,
      "buy_volume": buy_volume,
      "sell_volume": sell_volume,
      "delta": delta,
      "trade_count": trade_count,
    }
    
    producer.send("crypto.orderflow", event)
    print(event)
    
for message in consumer:
    trade = message.value
    process_trade(trade)