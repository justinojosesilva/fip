import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fip_postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fip_postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fip_postgres")

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")

BUFFER_SIZE = 5 #50 é o normal