import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fip_postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fip_postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fip_postgres")

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")

# Iceberg detection
ICEBERG_TRADE_THRESHOLD = 3

# Liquidity vacuum
VACUUM_ORDERFLOW_THRESHOLD = 0.65
VACUUM_SPREAD_THRESHOLD = 10

# Absorption
ABSORPTION_IMBALANCE_THRESHOLD = 0.20
ABSORPTION_DELTA_THRESHOLD = 0.5