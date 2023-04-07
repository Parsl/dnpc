import itertools
import matplotlib.pyplot as plt
import sqlite3
import sys
import dnpcsql.queries as queries

if __name__ == "__main__":

    if len(sys.argv) == 2:
        root_span_type = sys.argv[1]
    else:
        root_span_type = "parsl.monitoring.task"
    print(f"Looking for events rooted in span type {root_span_type}")

    # make a plot over time of how many tasks are in each state

    query = queries.events_for_root_span_type(root_span_type)

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

    # check we end up with the same number of events after grouping...
    # because i am seeing weird behaviour:
    accumulator = 0
    for g in groups:
        accumulator += len(g)

    assert accumulator == len(rows)

    print(f"There are {len(groups)} root spans (parsl tasks)")

    print("This is what group 0 looks like:")
    print(groups[0])

    # turn each event into a "+1, -1"-style counter modifier

    events = []

    for g in groups:
        print(f"==== group with root span uuid {g[0][0]}")
        g.sort(key=lambda r: r[1])
        last_event_name = None

        started = False
        cut = False
        for (root_span_uuid, event_time, span_type, event_type, event_uuid) in g:
            event_name = span_type + "/" + event_type

            should_start = True

            # should_start = span_type == "workqueue.task" and event_type == "RUNNING"
            # should_start = span_type == "parsl.wqexecutor.remote" and event_type == "LOADFUNCTION_LOADPICKLED_FUNCTION"

            should_cut = False


            # should_cut1 = span_type == "parsl.monitoring.try" \
            #            and (event_type == "exec_done" or \
            #            event_type == "memo_done" or \
            #            event_type == "dep_fail" or \
            #            event_type == "failed") 
            # sould_cut = should_cut1 or (span_type == "parsl.wqexecutor.remote" and event_type == "EXECUTEFUNCTION")

            if should_start:
                started = True

            if cut or not started:
                pass
            elif should_cut and last_event_name:  # no more processing
                events.append((last_event_name, float(event_time), -1))
                cut = True
            elif should_cut:
                cut = True
            elif last_event_name and last_event_name != event_name:
                events.append((last_event_name, float(event_time), -1))
                events.append((event_name, float(event_time), 1))
                last_event_name = event_name
            elif last_event_name is None:
                events.append((event_name, float(event_time), 1))
                last_event_name = event_name
            else:  # last_event_name was specified but this is a transition to the same state, so ignore
                pass

    print(f"There are {len(events)} counter changes.") 

    # sort counter modifiers by time and split/group by event type

    events.sort(key=lambda r: r[0])
    grouped_by_event_name = itertools.groupby(events, lambda r: r[0])

    grouped_by_event_name = [(event_name, list(g)) for (event_name, g) in grouped_by_event_name]

    print(f"There are {len(grouped_by_event_name)} event groups.")

    # for each event type, fold over counter modifier

    unified_x_axis_values = set()

    accumulated_changes_by_event_name = {}

    for (event_name, counter_events) in grouped_by_event_name:
        print(f"{event_name} has {len(counter_events)} counter-events")
        counter_events.sort(key=lambda r: r[1])
        accumulator = 0
        accumulated_changes = []
        for (event_name, event_time, delta) in counter_events:
            accumulator += delta
            accumulated_changes.append((event_time, accumulator))
            unified_x_axis_values.add(event_time)
            assert accumulator >= 0, f"Event {event_name} count became negative at {event_time}"

        accumulated_changes_by_event_name[event_name] = accumulated_changes

    sorted_unified_x_axis_values = list(unified_x_axis_values)
    sorted_unified_x_axis_values.sort()

    # now flesh out each sequence with the unified x axis
    for event_name in accumulated_changes_by_event_name.keys():
        accumulated_changes = accumulated_changes_by_event_name[event_name]
        existing_x_keys = set([e[0] for e in accumulated_changes])
        print(f"There are {len(existing_x_keys)} in {event_name} counter sequence")
        remaining_x_keys = unified_x_axis_values.difference(existing_x_keys)
        print(f"Need to augment {len(remaining_x_keys)} additional x keys")

        new_accumulated_changes = []
        last_counter = 0
        for x in sorted_unified_x_axis_values:
            if x in existing_x_keys:
                es = [e[1] for e in accumulated_changes if e[0] == x]

                # this is a bit of an awkward situation because we dont' know which came first...
                # so don't know which value to carry forwards...
                assert len(es) < 2, "found more than one event at this timestamp... possible but maybe unusual? " + str(es)

                assert len(es) > 0, "couldn't find an event we were expecting"
                assert len(es) == 1
                new_accumulated_changes.extend(es)
                last_counter = es[0]
            else:
                new_accumulated_changes.extend([last_counter])

        for v in new_accumulated_changes:
            assert isinstance(v, int)

        accumulated_changes_by_event_name[event_name] = new_accumulated_changes

        print(f"Now have {len(new_accumulated_changes)} for this event name")

        assert len(new_accumulated_changes) == len(sorted_unified_x_axis_values)

    # make a unified x axis because stackplot wants a single x axis
    # (at least, last time I did this, this is how I did it)

    # plot each event type on a stack plot.
    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(1, 1, 1)

    ax.stackplot(sorted_unified_x_axis_values, list(accumulated_changes_by_event_name.values()), labels=list(accumulated_changes_by_event_name.keys()))   # , labels=labels, colors=colors, baseline=baseline)
    ax.legend(loc='upper left', bbox_to_anchor=(1,1))
    plt.title("Root spans in which state over time")

    # shrink the plot by 30% to give more room for usually wide labels
    # https://stackoverflow.com/questions/4700614/how-to-put-the-legend-outside-the-plot
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])

    plt.savefig("stacktemp.png")

