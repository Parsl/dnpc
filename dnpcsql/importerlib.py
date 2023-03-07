import datetime
import uuid

from typing import Dict, TypeVar

X = TypeVar('X')

def local_key_to_span_uuid(*,
                           cursor,
                           local_key: X,
                           namespace: Dict[X, str],
                           span_type: str,
                           description: str) -> str:
    """Makes sure a span exists for given key. If the key is in namespace
    already, the span uuid is returned, like a regular dictionary lookup in
    the namespace. If it is not in the namespace, a new span is generated and
    inserted into the database.
    Repeatedly calling local_key_to_span_uuid with the same local_key and
    namespace will always return the same span uuid, which will be present in
    the span table of the database.
    """

    if local_key not in namespace:
        span_uuid = str(uuid.uuid4())
        namespace[local_key] = span_uuid
        cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (span_uuid, span_type, description))
    else:
        span_uuid = namespace[local_key]

    return span_uuid

def store_event(*,
                cursor,
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


def logfile_time_to_unix(s: str) -> float:
    """Converts a parsl logfile timestamp like 2023-03-06 11:20:17.282
    to a unix time"""
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").timestamp()
