from common.base_engine import BaseEngine
from common.state_store import StateStore

class StatefulBaseEngine(BaseEngine):
    def __init__(self, kafka_server):
        super().__init__(kafka_server)
        self.state = StateStore()
        
    def get_state(self, key):
        return self.state.get(key)
        
    def set_state(self, key, value):
        self.state.set(key, value)