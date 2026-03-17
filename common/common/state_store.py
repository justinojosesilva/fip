import json
import redis

class StateStore:
  def __init__(self, host="redis", port=6379):
      self.redis = redis.Redis(host=host, port=port, decode_responses=True)
      
  def get(self, key):
      data = self.redis.get(key)
      if not data:
        return None
      
      return json.loads(data)
      
  def set(self, key, value):
      self.redis.set(key, json.dumps(value))
      
  def delete(self, key):
      self.redis.delete(key)