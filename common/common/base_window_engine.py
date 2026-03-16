from datetime import datetime
import time

from common.base_engine import BaseEngine

class BaseWindowEngine(BaseEngine):
    window_seconds = 60
    key_field = "symbol"
    
    def __init__(self, kafka_server):
        super().__init__(kafka_server)
        self.windows = {}
        
    def get_key(self, data):
        return data[self.key_field]
        
    def get_window(self, timestamp):
        ts = datetime.fromisoformat(timestamp).timestamp()
        return int(ts // self.window_seconds)
        
    def process(self, event):
        data = event["data"]
        key = self.get_key(data)
        window = self.get_window(data["time"])
        window_key = f"{key}-{window}"
        
        if window_key not in self.windows:
          self.windows[window_key] =  []
        
        self.windows[window_key].append(data)
        
        return self.flush_windows()
        
    def flush_windows(self):
        now_window = int(time.time() //self.window_seconds)
        results = []
        keys_to_delete = []
        
        for window_key in self.windows:
            key, window = window_key.split("-")
            window = int(window)
            
            if window < now_window:
               events = self.windows[window_key]
               result = self.process_window(
                 key,
                 window,
                 events
               )
               
               if result:
                  results.append(result)
                
               keys_to_delete.append(window_key)
        
        for key in keys_to_delete:
            del self.windows[key]
        
        return results
    
    def process_window(self, key, window, events):
        
        raise NotImplementedError(
          "Subclasses must implement process_window"
        )