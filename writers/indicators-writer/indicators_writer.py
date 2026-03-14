import json
import psycopg2
import time
from datetime import datetime
from kafka import KafkaConsumer

import config


def connect_consumer():

    while True:
        try:

            consumer = KafkaConsumer(
                "crypto.indicators.metrics",
                bootstrap_servers=config.KAFKA_SERVER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
            )

            print("Connected to Kafka")

            return consumer

        except:

            print("Kafka not ready...")
            time.sleep(5)


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


consumer = connect_consumer()

conn = connect_db()
cursor = conn.cursor()


def save_indicator(event):

    try:

        time_value = datetime.fromisoformat(event["time"])

        cursor.execute(
            """
            INSERT INTO analytics.crypto_indicators
            (time, symbol, price, ema, rsi, vwap, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                time_value,
                event["symbol"],
                event["price"],
                event["ema"],
                event["rsi"],
                event["vwap"],
                event["volume"]
            )
        )

        conn.commit()

    except Exception as e:

        print("Database error:", e)
        conn.rollback()


for message in consumer:

    event = message.value

    save_indicator(event)

    print("Saved indicators:", event)