import uuid
from datetime import datetime, timezone

def create_event(event_type, source, data):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source": source,
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data
    }