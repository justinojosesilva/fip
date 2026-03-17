import config
from common.kafka import KafkaClient
from common.event import create_event
from common.stream_engine import StreamEngine

kafka = KafkaClient(config.KAFKA_SERVER)

topics = [
  "crypto.orderflow.metrics",
  "crypto.orderbook.metrics",
  "crypto.trades"
]

engine = StreamEngine(
  config.KAFKA_SERVER,
  topics,
  "microstructure-engine",
  window_seconds=5
)

# -----------------------------
# STATE (para filtros avançados)
# -----------------------------

volume_state = {}
pattern_state = {}

# -----------------------------
# UTILIDADES
# -----------------------------

def update_volume_state(symbol, volume):

    if symbol not in volume_state:
        volume_state[symbol] = []

    volume_state[symbol].append(volume)

    if len(volume_state[symbol]) > 50:
        volume_state[symbol].pop(0)


def get_avg_volume(symbol):

    vols = volume_state.get(symbol, [])

    if len(vols) == 0:
        return 0

    return sum(vols) / len(vols)


def confirm_pattern(symbol, pattern):

    key = f"{symbol}:{pattern}"

    if key not in pattern_state:
        pattern_state[key] = 1
    else:
        pattern_state[key] += 1

    if pattern_state[key] >= config.PATTERN_CONFIRMATION:

        pattern_state[key] = 0
        return True

    return False


# -----------------------------
# ENGINE
# -----------------------------

def detect(symbol, streams, window):

    if "crypto.orderflow.metrics" not in streams:
        return

    if "crypto.orderbook.metrics" not in streams:
        return

    orderflow = streams["crypto.orderflow.metrics"]
    orderbook = streams["crypto.orderbook.metrics"]

    update_volume_state(symbol, abs(orderflow["delta"]))

    signals = []

    signals += detect_absorption(symbol, orderflow, orderbook)
    signals += detect_liquidity_vacuum(symbol, orderflow, orderbook)
    signals += detect_iceberg(symbol, window)
    signals += detect_aggressive_sweep(symbol, window)

    for signal in signals:

        event = create_event(
            event_type="crypto.microstructure.signals",
            source="microstructure-engine",
            data=signal
        )

        print("Microstructure signal:", event)

        kafka.publish("crypto.microstructure.signals", event)


# -----------------------------
# ABSORPTION
# -----------------------------

def detect_absorption(symbol, orderflow, orderbook):

    signals = []

    if (
        abs(orderflow["delta"]) > config.ABSORPTION_DELTA_THRESHOLD
        and abs(orderbook["imbalance"]) < config.ABSORPTION_IMBALANCE_THRESHOLD
    ):

        if confirm_pattern(symbol, "ABSORPTION"):

            confidence = min(abs(orderflow["delta"]) / 5, 1.0)

            signals.append({
                "symbol": symbol,
                "type": "ABSORPTION",
                "delta": orderflow["delta"],
                "imbalance": orderbook["imbalance"],
                "confidence": confidence
            })

    return signals


# -----------------------------
# LIQUIDITY VACUUM
# -----------------------------

def detect_liquidity_vacuum(symbol, orderflow, orderbook):

    signals = []

    if (
        orderbook["spread"] > config.VACUUM_SPREAD_THRESHOLD
        and abs(orderflow["imbalance"]) > config.VACUUM_ORDERFLOW_THRESHOLD
    ):

        if confirm_pattern(symbol, "VACUUM"):

            confidence = min(orderbook["spread"] / 10, 1.0)

            signals.append({
                "symbol": symbol,
                "type": "LIQUIDITY_VACUUM",
                "spread": orderbook["spread"],
                "orderflow": orderflow["imbalance"],
                "confidence": confidence
            })

    return signals


# -----------------------------
# ICEBERG
# -----------------------------

def detect_iceberg(symbol, window):

    signals = []
    trade_sizes = []

    for event in window:

        if event.get("event_type") != "crypto.trades":
            continue

        trade = event.get("data", {})

        if trade.get("symbol") != symbol:
            continue

        trade_sizes.append(float(trade.get("quantity", 0)))

    if len(trade_sizes) < 10:
        return signals

    avg_size = sum(trade_sizes) / len(trade_sizes)

    large_trades = [t for t in trade_sizes if t > avg_size * 5]

    if len(large_trades) > config.ICEBERG_TRADE_THRESHOLD:

        if confirm_pattern(symbol, "ICEBERG"):

            signals.append({
                "symbol": symbol,
                "type": "ICEBERG_ORDER",
                "large_trades": len(large_trades),
                "avg_trade_size": avg_size,
                "confidence": min(len(large_trades) / 10, 1.0)
            })

    return signals


# -----------------------------
# AGGRESSIVE SWEEP
# -----------------------------

def detect_aggressive_sweep(symbol, window):

    signals = []

    buy_volume = 0
    sell_volume = 0
    trade_count = 0

    for event in window:

        if event.get("event_type") != "crypto.trades":
            continue

        trade = event.get("data", {})

        if trade.get("symbol") != symbol:
            continue

        qty = float(trade.get("quantity", 0))
        side = trade.get("side")

        trade_count += 1

        if side == "buy":
            buy_volume += qty
        else:
            sell_volume += qty

    total_volume = buy_volume + sell_volume

    if trade_count < 10:
        return signals

    avg_volume = get_avg_volume(symbol)

    if avg_volume == 0:
        return signals

    relative_volume = total_volume / avg_volume

    if relative_volume < config.SWEEP_RELATIVE_VOLUME:
        return signals

    buy_ratio = buy_volume / total_volume if total_volume > 0 else 0

    if buy_ratio > config.SWEEP_BUY_RATIO:

        if confirm_pattern(symbol, "BUY_SWEEP"):

            signals.append({
                "symbol": symbol,
                "type": "AGGRESSIVE_BUY_SWEEP",
                "volume": total_volume,
                "relative_volume": relative_volume,
                "trade_count": trade_count,
                "confidence": min(relative_volume / 5, 1.0)
            })

    if buy_ratio < (1 - config.SWEEP_BUY_RATIO):

        if confirm_pattern(symbol, "SELL_SWEEP"):

            signals.append({
                "symbol": symbol,
                "type": "AGGRESSIVE_SELL_SWEEP",
                "volume": total_volume,
                "relative_volume": relative_volume,
                "trade_count": trade_count,
                "confidence": min(relative_volume / 5, 1.0)
            })

    return signals


engine.run(detect)