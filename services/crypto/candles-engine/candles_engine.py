import config
from datetime import datetime
from common.event import create_event
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)

current_candle = None
current_window = None


def start_new_candle(price, qty, symbol):
    return {
        "symbol": symbol,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": qty,
    }


for event in kafka.consume("crypto.trades", "orderflow-engine"):
    trade = event["data"]

    symbol = trade["symbol"]
    price = float(trade["price"])
    qty = float(trade["quantity"])

    trade_time = datetime.fromisoformat(trade["time"]).timestamp()

    window = int(trade_time // config.CANDLE_INTERVAL)

    if current_window is None:
      
        current_window = window
        current_candle = start_new_candle(price, qty, symbol)

    elif window != current_window:
        candle_event = create_event(
          event_type="candles",
          source="candles-engine",
          data={
              "symbol": current_candle["symbol"],
              "interval": "1m",
              "open": current_candle["open"],
              "high": current_candle["high"],
              "low": current_candle["low"],
              "close": current_candle["close"],
              "volume": current_candle["volume"],
              "time": datetime.utcfromtimestamp(
                  current_window * config.CANDLE_INTERVAL
              ).isoformat(),
          }
        )
        print("Candle: ", candle_event)
        kafka.publish("crypto.candles", candle_event)
        
        current_window = window
        current_candle = start_new_candle(price, qty, symbol)

    else:
        current_candle["high"] = max(current_candle["high"], price)
        current_candle["low"] = min(current_candle["low"], price)
        current_candle["close"] = price
        current_candle["volume"] += qty
