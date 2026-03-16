import json
import time
from kafka import KafkaProducer, KafkaConsumer

class KafkaClient:
  
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    def connect_producer(self):
        
        while True:
            try:
                self.producer = KafkaProducer(
                  bootstrap_servers=self.bootstrap_servers,
                  value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                
                print("Kafka Producer connected successfully.")
                return self.producer
            except Exception as e:
                print(f"Kafka producer connection failed: {e}")
                print("Retrying in 5 seconds...")
                time.sleep(5)
                
    def publish(self, topic, event):
        if not self.producer:
            self.connect_producer()
            
        try:
          self.producer.send(topic, event)
        
        except Exception as e:
          print(f"Failed to publish message: {e}")
                
    def create_consumer(self, topics, group_id=None):
      
      if isinstance(topics, str):
          topics = [topics]
      
      while True:
          try:
              consumer = KafkaConsumer(
                  *topics,
                  bootstrap_servers=self.bootstrap_servers,
                  value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                  group_id=group_id,
                  auto_offset_reset="earliest",
                  enable_auto_commit=True
              )
              print("Kafka Consumer connected successfully.")
              return consumer
          except Exception as e:
              print(f"Kafka consumer connection failed: {e}")
              print("Retrying in 5 seconds...")
              time.sleep(5)
              
    def consume(self, topic, group_id=None):
        consumer = self.create_consumer(topic, group_id)
        for message in consumer:
            value = message.value
            if isinstance(value, list):
                for event in value:
                  yield event
            else:
              yield value