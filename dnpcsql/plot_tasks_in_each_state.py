import itertools
import sqlite3
import dnpcsql.queries as queries

if __name__ == "__main__":

    # make a plot over time of how many tasks are in each state

    query = queries.events_for_root_span_type("parsl.monitoring.task")

    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    cursor = db.cursor()

    # select descs.root_span_uuid, event.time, span.type, event.type, event.uuid
    rows = list(cursor.execute(query))

    # get each task's flattened event stream, grouped by task span

    rows.sort(key=lambda r: r[0])
    groups = itertools.groupby(rows, lambda r: r[0])

    # Concretise the two levels of iterators returned by itertools into
    # list objects.
    groups = [(list(g)) for (_uuid, g) in groups]

    print(f"There are {len(groups)} root spans (parsl tasks)")

    print("This is what group 0 looks like:")
    print(groups[0])

    # turn each event into a "+1, -1"-style counter modifier

    events = []

    for g in groups:
        print("====")
        last_event_name = None
        for (root_span_uuid, event_time, span_type, event_type, event_uuid) in g:
            event_name = span_type + "/" + event_type
            if last_event_name:
                print(f"reducing last event: {last_event_name}")
                events.append( (last_event_name, event_time, -1) )
            print(f"increasing next event: {event_name}")
            events.append( (event_name, event_time, 1) )
            last_event_name = event_name

    print(f"There are {len(events)} counter changes.") 

    # sort counter modifiers by time and split/group by event type

    events.sort(key=lambda r: r[0])
    grouped_by_event_name = itertools.groupby(events, lambda r: r[0])

    grouped_by_event_name = [(event_name, list(g)) for (event_name, g) in grouped_by_event_name]

    print(f"There are {len(grouped_by_event_name)} event groups.")

    # for each event type, fold over counter modifier

    for (event_name, counter_events) in grouped_by_event_name:
        print(f"{event_name} has {len(counter_events)} counter-events")

# plot each event type on a stack plot.
