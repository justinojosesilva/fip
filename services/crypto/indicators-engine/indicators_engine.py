import json
import time
from datetime import datetime, timezone

import config
import psycopg2
from kafka import KafkaConsumer

buffers = {}
BUFFER_SIZE = 50

symbol_state = {}


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


def calculate_ema(prices, period=config.EMA_PERIOD):
    if len(prices) < period:
        return None
    multipler = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = (price - ema) * multipler + ema
    return ema

def calculate_rsi(prices, period=config.RSI_PERIOD):
    if len(prices) < period + 1:
        return None
        
    gains = []
    losses = []
    
    for i in range(1, period + 1):
      
        diff = prices[i] - prices[i - 1]
        
        if diff >= 0:
          gains.append(diff)
        else:
          losses.append(abs(diff))
          
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    
    if avg_loss == 0:
        return 100
        
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi
    
def calculate_vwap(prices, quantities):
    total_volume = sum(quantities)
    if total_volume == 0:
      return None
      
    vwap = sum(p * q for p, q in zip(prices, quantities)) / total_volume
    return vwap

def process_trade(trade):

    symbol = trade["symbol"]
    price = float(trade["price"])
    quantity = float(trade["quantity"])
    time = datetime.fromisoformat(trade["time"])

    if symbol not in buffers:
        buffers[symbol] = []
    
    buffers[symbol].append({
      "price": price,
      "quantity": quantity
    })
    
    if len(buffers[symbol]) < BUFFER_SIZE:
        return
      
    trades = buffers[symbol]
    
    prices = [t["price"] for t in trades]
    quantities = [t["quantity"] for t in trades]
    
    rsi = calculate_rsi(prices)
    ema = calculate_ema(prices)
    vwap = calculate_vwap(prices, quantities)
    
    save_indicators(symbol, price, ema, rsi, vwap, sum(quantities))
    
    buffers[symbol] = []


def save_indicators(symbol, price, ema, rsi, vwap, volume):
    
    now = datetime.now(timezone.utc)
  
    try:
        cursor.execute(
            """
      INSERT INTO analytics.crypto_indicators
      (time, symbol, price, ema, rsi, vwap, volume)
      VALUES (%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT (time, symbol)
      DO UPDATE SET
          price = EXCLUDED.price,
          rsi = EXCLUDED.rsi,
          ema = EXCLUDED.ema,
          vwap = EXCLUDED.vwap;
      """,
            (now, symbol, price, ema, rsi, vwap, volume),
        )
        conn.commit()
    except Exception as e:
        print(f"DB error: {e}")
        conn.rollback()


for message in consumer:
    trade = message.value
    process_trade(trade)
