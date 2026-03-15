import json
import time
import config
from common.kafka import KafkaClient
from common.event import create_event

kafka = KafkaClient(config.KAFKA_SERVER)

orderflow_data = {}
orderbook_data = {}
liquidation_data = {}

# kafka
def connect_kafka():
    while True:
      try:
          consumer = KafkaConsumer(
              "crypto.orderflow",
              "crypto.orderbook.raw",
              "crypto.liquidation.metrics",
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

def detect_squeeze(symbol):
    if symbol not in orderflow_data:
        return
    if symbol not in orderbook_data:
        return
    if symbol not in liquidation_data:
        return
    orderflow = orderflow_data[symbol]
    orderbook = orderbook_data[symbol]
    liquidation = liquidation_data[symbol]
    
    buy_pressure = orderflow["buy_ratio"]
    sell_pressure = orderflow["sell_ratio"]
    
    imbalance = orderbook["imbalance"]
    
    long_liq = liquidation["long_liquidations"]
    short_liq = liquidation["short_liquidations"]
    
    # SHORT SQUEEZE
    if (
      short_liq > config.SHORT_SQUEEZE_THRESHOLD 
      and buy_pressure > config.ORDERFLOW_THRESHOLD
      and imbalance > config.ORDERBOOK_IMBALANCE_THRESHOLD
    ):
      signal = create_event(
        event_type="squeeze_signal",
        source="squeeze_detector_engine",
        data={
          "symbol": symbol,
          "type": "LONG_SQUEEZE",
          "confidence": sell_pressure,
          "time": liquidation["time"]
        }
      )
      
      print(f"Detected LONG SQUEEZE for {symbol} with confidence {sell_pressure:.2f}")
      kafka.publish("crypto.squeeze.signals", signal)

for event in kafka.consume(["crypto.orderflow", "crypto.orderbook.raw", "crypto.liquidation.metrics"], "squeeze-detector-engine"):
    topic = event["topic"]
    data = event["data"]
    symbol = data["symbol"]
    
    if topic == "crypto.orderflow":
        orderflow_data[symbol] = data
    elif topic == "crypto.orderbook.raw":
        orderbook_data[symbol] = data
    elif topic == "crypto.liquidation.metrics":
        liquidation_data[symbol] = data
        
    detect_squeeze(symbol)  