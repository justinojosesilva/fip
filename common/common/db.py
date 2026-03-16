import time
import psycopg2
from psycopg2.pool import SimpleConnectionPool

class Database:
    def __init__(self, config):
      while True:
        try:
            self.pool = SimpleConnectionPool(
                1, 
                10,
                host=config.POSTGRES_HOST,
                database=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD
            )
            print("Database connection pool created successfully")
            break
        except Exception as e:
          print(f"Error creating database connection pool: {e}")
          print("Retrying in 5 seconds...")
          time.sleep(5)

    def connect(self):
        return self.pool.getconn()

    def release(self, conn):
        self.pool.putconn(conn)