from common.stream_processor import StreamProcessor


class BaseEngine(StreamProcessor):
    source = "engine"

    def __init__(self, kafka_server):
        super().__init__(kafka_server)

    def build_event(self, event_type, data):
        from common.event import create_event

        return create_event(event_type=event_type, source=self.source, data=data)
