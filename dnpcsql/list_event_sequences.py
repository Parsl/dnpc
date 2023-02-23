# List the order in which events happen to parsl tasks and their
# (recursive) subspans
# Maybe list them in mean-order-since-start-of-task?

import itertools
import sqlite3

if __name__ == "__main__":

    query = """
with recursive

  descs(root_span_uuid, span_uuid) as (
    select span.uuid, span.uuid from span where span.type="parsl.task"
    union
    select descs.root_span_uuid, subspan.subspan_uuid
      from subspan, descs
     where subspan.superspan_uuid = descs.span_uuid
  )

  select descs.root_span_uuid, event.time, span.type, event.type
    from descs, span, event
   where span.uuid = descs.span_uuid
     and event.span_uuid = span.uuid
order by root_span_uuid, event.time;
    """

    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    cursor = db.cursor()

    rows = list(cursor.execute(query))

    groups = itertools.groupby(rows, lambda r: r[0])

    hash_counts = {}
    hash_sequences = {}

    for (root_span_uuid, events) in groups:
      events = list(events)
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
          hash_sequences[h] = []
      hash_counts[h] += 1
      hash_sequences[h].append(events)
      print("=====")
    print(f"There were {len(hash_counts)} different orderings of events:")
    print(hash_counts)

    most_common_count = max(hash_counts.values())
    print(f"Most common count: {most_common_count}")

    most_common_hash = [k for k in hash_counts.keys() if hash_counts[k] == most_common_count][0]

    print(f"Most common hash: {most_common_hash}")

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
      print(f"Template event:  {event_time} {span_type}/{event_type}")
      template_events.append(0)

    for s in hash_sequences[most_common_hash]:
      last_time = float(s[0][1])
      n = 0
      for e in s:
        event_time = float(e[1])
        time_since_last = event_time - last_time
        template_events[n] += time_since_last
        last_time = event_time
        n += 1

    n=0
    for e in example_events:
      event_time=e[1]
      span_type=e[2]
      event_type=e[3]
      cumul_time = template_events[n] / most_common_count
      print(f"Mean time since prev.:  {cumul_time} {span_type}/{event_type}")
      n += 1


