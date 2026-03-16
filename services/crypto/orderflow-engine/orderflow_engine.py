from datetime import datetime, timezone

import config

from common.base_engine import BaseEngine


class OrderFlowEngine(BaseEngine):
    input_topic = "crypto.trades"
    output_topic = "crypto.orderflow.metrics"
    group_id = "orderflow-engine"
    source = "orderflow-engine"

    BUFFER_SIZE = config.BUFFER_SIZE

    def __init__(self, kafka_server):
        super().__init__(kafka_server)
        self.buffers = {}
        self.cvd_state = {}

    def process(self, event):
        trade = event["data"]
        symbol = trade["symbol"]

        price = float(trade["price"])
        quantity = float(trade["quantity"])
        side = trade["side"]

        if symbol not in self.buffers:
            self.buffers[symbol] = []

        self.buffers[symbol].append(
            {
                "price": price,
                "quantity": quantity,
                "side": side,
            }
        )

        if len(self.buffers[symbol]) >= config.BUFFER_SIZE:
            return self.calculate_orderflow(symbol)

    def calculate_orderflow(self, symbol):

        trades = self.buffers[symbol]

        buy_volume = 0
        sell_volume = 0
        total_volume = 0

        for trade in trades:
            qty = trade["quantity"]
            total_volume += qty

            if trade["side"] == "buy":
                buy_volume += qty
            else:
                sell_volume += qty

        delta = buy_volume - sell_volume

        # 🔹 CVD acumulado
        if symbol not in self.cvd_state:
            self.cvd_state[symbol] = 0

        self.cvd_state[symbol] += delta
        cvd = self.cvd_state[symbol]

        trade_count = len(trades)

        avg_trade_size = total_volume / trade_count if trade_count > 0 else 0

        # 🔹 pressão compradora
        buy_ratio = buy_volume / total_volume if total_volume > 0 else 0

        # 🔹 imbalance
        imbalance = delta / total_volume if total_volume > 0 else 0

        metric = {
            "symbol": symbol,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "delta": delta,
            "cvd": cvd,
            "buy_ratio": buy_ratio,
            "imbalance": imbalance,
            "trade_count": trade_count,
            "avg_trade_size": avg_trade_size,
            "time": datetime.now(timezone.utc).isoformat(),
        }

        print("Orderflow:", metric)

        # mantém janela deslizante
        self.buffers[symbol] = self.buffers[symbol][1:]

        return self.build_event("crypto.orderflow.metrics", metric)


engine = OrderFlowEngine(config.KAFKA_SERVER)

engine.run()
