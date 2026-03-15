import json
import time
import config
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
from common.event import create_event

buffers = {}
BUFFER_SIZE = 50

symbol_state = {}


# kafka Consumer
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


def calculate_ema(prices, period=config.EMA_PERIOD):
    if len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = (price - ema) * multiplier + ema
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

    event = create_event(
        event_type="indicators",
        source="indicators-engine",
        data={
          "symbol": symbol,
          "price": price,
          "ema": ema,
          "rsi": rsi,
          "vwap": vwap,
          "volume": sum(quantities),
          "time": datetime.now(timezone.utc).isoformat(),
        }
    )
    print("Indicators: ", event)
    producer.send("crypto.indicators.metrics", event)
    buffers[symbol] = []


for message in consumer:
    event = message.value
    trade = event["data"]
    process_trade(trade)
