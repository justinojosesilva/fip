from common.kafka import KafkaClient

class BaseKafka:
    def __init__(self, kafka_server):
        self.kafka = KafkaClient(kafka_server)
    
    def publish(self, topic, event):
        self.kafka.publish(topic, event)
        
    def consume(self, topic, group):
        return self.kafka.consume(topic, group)