import time
from collections import defaultdict, deque

class StreamWindow:
    def __init__(self, window_seconds):
      self.window_seconds = window_seconds
      self.events = defaultdict(deque)
      
    def add(self, symbol, event):
      now = time.time()
      self.events[symbol].append((now, event))
      self.cleanup(symbol)
      
    def cleanup(self, symbol):
      now = time.time()
      while self.events[symbol]:
        timestamp, _ = self.events[symbol][0]
        if now - timestamp > self.window_seconds:
          self.events[symbol].popleft()
        else:
          break
          
    def get(self, symbol):
      return [event for _, event in self.events[symbol]]