import json
import psycopg2
import time
import config
from datetime import datetime
from common.kafka import KafkaClient

kafka = KafkaClient(config.KAFKA_SERVER)

def connect_db():

    while True:
        try:

            conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                database=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD
            )

            print("Connected to PostgreSQL")

            return conn

        except psycopg2.OperationalError:

            print("Waiting for PostgreSQL...")
            time.sleep(3)

conn = connect_db()
cursor = conn.cursor()


def save_orderflow(event):
    try:
        time_value = datetime.fromisoformat(event["time"])
        cursor.execute(
            """
            INSERT INTO analytics.crypto_orderflow
            (time, symbol, buy_volume, sell_volume, delta, trade_count)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                time_value,
                event["symbol"],
                event["buy_volume"],
                event["sell_volume"],
                event["delta"],
                event["trade_count"]
            )
        )
        conn.commit()

    except Exception as e:
        print("Database error:", e)
        conn.rollback()


for event in kafka.consume("crypto.orderflow.metrics", "orderflow-writer"):
    data = event["data"]
    print("Saved orderflow:", event)
    save_orderflow(data)