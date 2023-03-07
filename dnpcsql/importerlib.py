import uuid

def store_event(*, cursor,
                   span_uuid: str,
                   event_time: float,
                   event_type: str,
                   description: str):
    """writes an event into the database"""

    event_uuid = str(uuid.uuid4())
    cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)",
                   (event_uuid,
                    span_uuid,
                    event_time,
                    event_type,
                    description)
                   )
