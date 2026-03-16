import logging
import time
from common.kafka import KafkaClient

logger = logging.getLogger("stream")

class StreamProcessor:
    input_topic = None
    output_topic = None
    group_id = None
    dlq_topic = None
    
    max_retries = 3
    retry_delay = 2
    
    def __init__(self, kafka_server):
        self.kafka = KafkaClient(kafka_server)
        
        self.metrics = {
          "processed": 0,
          "errors": 0,
          "retries": 0
        }
        
    def process(self, event):
        raise NotImplementedError("Subclasses must implement the process method")
        
    def handle_error(self, event, error):
        logger.error(f"Processing failed after retries: {error}")
        
        if self.dlq_topic:
           dlq_event = {
             "error": str(error),
             "event": event
           }
           self.kafka.publish(self.dlq_topic, dlq_event)
           
    def publish_result(self, result):
        
        if result is None or not self.output_topic:
           return
           
        if isinstance(result, list):
            for item in result:
               self.kafka.publish(self.output_topic, item)
        else:
            self.kafka.publish(self.output_topic, result)
    
    def run(self):
        
        logger.info(f"Starting processor {self.__class__.__name__}")
        
        for event in self.kafka.consume(self.input_topic, self.group_id):
          
            retries = 0
            
            while retries <= self.max_retries:
            
                try:
                    result = self.process(event)
                    self.publish_result(result)
                    self.metrics["processed"] += 1
                    break
                except Exception as e:
                    retries += 1
                    self.metrics["retries"] += 1
                    
                    logger.warning(
                        f"Retry {retries}/{self.max_retries} due to error: {e}"
                    )
                    
                    time.sleep(self.retry_delay)
                    
                    if retries > self.max_retries:
                       self.metrics["errors"] += 1
                       self.handle_error(event, e)
            if self.metrics["processed"] % 1000 == 0:
                
                logger.info(
                  f"Metrics: processed={self.metrics['processed']} "
                  f"errors={self.metrics['errors']} "
                  f"retries={self.metrics['retries']}"
                )
                  