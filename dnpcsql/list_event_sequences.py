# List the order in which events happen to parsl tasks and their
# (recursive) subspans
# Maybe list them in mean-order-since-start-of-task?

import itertools
import sqlite3
import sys

from typing import Any, List, Dict

import dnpcsql.queries as queries

if __name__ == "__main__":

    if len(sys.argv) == 2:
        root_span_type = sys.argv[1]
    else:
        root_span_type = "parsl.monitoring.task"

    print(f"Looking for events rooted in span type {root_span_type}")
    query = queries.events_for_root_span_type(root_span_type)

    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    cursor = db.cursor()

    rows = list(cursor.execute(query))

    groups = itertools.groupby(rows, lambda r: r[0])

    hash_counts = {}
    hash_sequences: Dict[int, List[Any]] = {}

    for (root_span_uuid, events_iterator) in groups:
      events = list(events_iterator)
      hash_material = ""
      for e in events:
          event_time=e[1]
          span_type=e[2]
          event_type=e[3]
          hash_material += f" {span_type} {event_type}"
      h = hash(hash_material)
      if h not in hash_counts:
          hash_counts[h] = 0
          hash_sequences[h] = []
      hash_counts[h] += 1
      hash_sequences[h].append(events)
    print(f"There were {len(hash_counts)} different orderings of events")

    most_common_count = max(hash_counts.values())
    print(f"Most common count: {most_common_count}")

    most_common_hash = [k for k in hash_counts.keys() if hash_counts[k] == most_common_count][0]

    print("Example event sequence in this most common hash:")

    assert len(hash_sequences[most_common_hash]) == most_common_count
    example_events = hash_sequences[most_common_hash][0]

    # for each event in sequence, store cumulative time to next
    # no need to store count, as it should be constant most_common_count
    template_events = []

    for e in example_events:
      event_time=e[1]
      span_type=e[2]
      event_type=e[3]
      event_uuid=e[4]
      template_events.append(0)

    for s in hash_sequences[most_common_hash]:
      last_time = float(s[0][1])
      n = 0
      for e in s:
        if example_events[n][3] != e[3]:
            raise RuntimeError(f"Implementation error: this event is not in template sequence: template event type at this position: {example_events[n][3]}, this event type {e[3]}")
        event_time = float(e[1])
        time_since_last = event_time - last_time
        template_events[n] += time_since_last
        last_time = event_time
        n += 1

    print("Mean times for most common event sequence (cumul, inter-event)")
    n=0
    c: float = 0
    for e in example_events:
      event_time=e[1]
      span_type=e[2]
      event_type=e[3]
      inter_time = template_events[n] / most_common_count
      c += inter_time

      inter_time_formatted = "{:15.9f}".format(inter_time)
      cumul_time_formatted = "{:15.9f}".format(c)

      print(f"{cumul_time_formatted} {inter_time_formatted} {span_type}/{event_type}")
      n += 1


