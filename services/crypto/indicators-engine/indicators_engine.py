import config
from datetime import datetime
from common.event import create_event
from common.base_engine import BaseEngine


class IndicatorsEngine(BaseEngine):

    input_topic = "crypto.candles"
    output_topic = "crypto.indicators"
    group_id = "indicators-engine"
    source = "indicators-engine"

    buffers = {}
    
    BUFFER_SIZE = 50

    def process(self, event):

        candle = event["data"]

        symbol = candle["symbol"]
        close = float(candle["close"])
        volume = float(candle["volume"])

        if symbol not in self.buffers:
            self.buffers[symbol] = []

        self.buffers[symbol].append({
            "price": close,
            "volume": volume
        })

        # limita histórico
        if len(self.buffers[symbol]) > self.BUFFER_SIZE:
            self.buffers[symbol].pop(0)

        prices = [c["price"] for c in self.buffers[symbol]]
        volumes = [c["volume"] for c in self.buffers[symbol]]

        if len(prices) < config.RSI_PERIOD + 1:
            return None

        rsi = self.calculate_rsi(prices)
        ema = self.calculate_ema(prices)
        vwap = self.calculate_vwap(prices, volumes)

        indicator = {
            "symbol": symbol,
            "price": close,
            "ema": ema,
            "rsi": rsi,
            "vwap": vwap,
            "volume": sum(volumes),
            "time": candle["time"]
        }

        return self.build_event(
            "crypto.indicators",
            indicator
        )

    def calculate_ema(self, prices, period=config.EMA_PERIOD):

        if len(prices) < period:
            return None

        multiplier = 2 / (period + 1)

        ema = prices[0]

        for price in prices[1:]:
            ema = (price - ema) * multiplier + ema

        return ema

    def calculate_rsi(self, prices, period=config.RSI_PERIOD):

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

        return 100 - (100 / (1 + rs))

    def calculate_vwap(self, prices, volumes):

        total_volume = sum(volumes)

        if total_volume == 0:
            return None

        return sum(p * v for p, v in zip(prices, volumes)) / total_volume
        
engine = IndicatorsEngine(config.KAFKA_SERVER)

engine.run()
