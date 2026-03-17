import config
from datetime import datetime, timezone
from common.stateful_engine import StatefulBaseEngine


class OrderFlowEngine(StatefulBaseEngine):
    input_topic = "crypto.trades"
    output_topic = "crypto.orderflow.metrics"
    group_id = "orderflow-engine"
    source = "orderflow-engine"

    BUFFER_SIZE = config.BUFFER_SIZE

    def __init__(self, kafka_server):
        super().__init__(kafka_server)
        self.buffers = {}

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

        # recupera CVD salvo
        cvd = self.get_state(f"cvd:{symbol}")
        if not cvd:
            cvd = 0
            
        cvd += delta
        
        # salvar CVD
        self.set_state(f"cvd:{symbol}", cvd)

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
