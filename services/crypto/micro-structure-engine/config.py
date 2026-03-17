import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fip_postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fip_postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fip_postgres")

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")

# confirmação de padrões
PATTERN_CONFIRMATION = 2

# sweep detection
SWEEP_BUY_RATIO = 0.8
SWEEP_RELATIVE_VOLUME = 3

# iceberg
ICEBERG_TRADE_THRESHOLD = 3

# absorption
ABSORPTION_DELTA_THRESHOLD = 0.01
ABSORPTION_IMBALANCE_THRESHOLD = 0.3

# liquidity vacuum
VACUUM_SPREAD_THRESHOLD = 5
VACUUM_ORDERFLOW_THRESHOLD = 0.6