from collections import defaultdict

class StreamJoin:
    def __init__(self):
      self.state = defaultdict(dict)
      
    def update(self, topic, symbol, data):
      self.state[symbol][topic] = data
      
    def ready(self, symbol, topics):
      if symbol not in self.state:
        return False
      for topic in topics:
        if topic not in self.state[symbol]:
          return False
      return True
      
    def get(self, symbol):
      return self.state[symbol]