# List the order in which events happen to parsl tasks and their
# (recursive) subspans
# Maybe list them in mean-order-since-start-of-task?

import itertools
import sqlite3

if __name__ == "__main__":

    query = """
with recursive descs(root_span_uuid, span_uuid) as (select span.uuid, span.uuid from span where span.type="parsl.task" union select descs.root_span_uuid, subspan.subspan_uuid from subspan, descs where subspan.superspan_uuid = descs.span_uuid) select descs.root_span_uuid, event.time, span.type, event.type from descs, span, event where span.uuid = descs.span_uuid and event.span_uuid = span.uuid order by root_span_uuid, event.time;
    """

    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    cursor = db.cursor()

    rows = list(cursor.execute(query))

    groups = itertools.groupby(rows, lambda r: r[0])

    hash_counts = {}

    for (root_span_uuid, events) in groups:
      hash_material = ""
      print(f"Root span uuid {root_span_uuid}:")
      for e in events:
          event_time=e[1]
          span_type=e[2]
          event_type=e[3]
          print(f"  {event_time} {span_type}/{event_type}")
          hash_material += " {event_time} {span_type} {event_type}"
      h = hash(hash_material)
      print(f"hash of this sequence: {h}")
      if h not in hash_counts:
          hash_counts[h] = 0
      hash_counts[h] += 1
      print("=====")
    print(f"There were {len(hash_counts)} different orderings of events:")
    print(hash_counts)
