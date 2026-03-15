from common.kafka import KafkaClient
from common.stream_join import StreamJoin
from common.stream_window import StreamWindow

class StreamEngine:
    def __init__(self, kafka_server, topics, group_id, window_seconds=5):
        self.kafka = KafkaClient(kafka_server)
        self.consumer = self.kafka.create_consumer(topics, group_id)
        
        self.topics = topics
        self.join = StreamJoin()
        self.window = StreamWindow(window_seconds)
        
    def run(self, handler):
        for message in self.consumer:
            topic = message.topic
            event = message.value
            
            data = event["data"]
            symbol = data["symbol"]
            
            self.join.update(topic, symbol, data)
            
            self.window.add(symbol, {topic: data})
            
            if self.join.ready(symbol, self.topics):
                joined = self.join.get(symbol)
                window = self.window.get(symbol)
                handler(symbol, joined, window)