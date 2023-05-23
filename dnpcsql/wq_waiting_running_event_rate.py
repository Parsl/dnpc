import sqlite3
import numpy as np

# for a single event type, plot a histogram at 1 second resolution
# of how often that event happens.
# (so, the height of the bin should be the number of events per second, and
# each bin represents one second)

# TODO: this appears in multiple places: refactor into a db module
def init_sql() -> sqlite3.Connection:
    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    return db



db = init_sql()
cursor = db.cursor()

parsl_running_completed = """
    SELECT event.time
      FROM event, span
     WHERE event.type = "RUNNING"
       AND event.span_uuid = span.uuid
       AND span.type = "workqueue.task"
    """

parsl_WAITING_completed = """
    SELECT event.time
      FROM event, span
     WHERE event.type = "WAITING"
       AND event.span_uuid = span.uuid
       AND span.type = "workqueue.task"
    """


running_rows = list(cursor.execute(parsl_running_completed))
running_data = np.array([float(row[0]) for row in running_rows])
waiting_rows = list(cursor.execute(parsl_WAITING_completed))
waiting_data = np.array([float(row[0]) for row in waiting_rows])

# print(f"there are {len(rows)} relevant events in the db")

import matplotlib.pyplot as plt

def plot_histo(ydata, ydata2, output_filename, event_name):

    bins = int(ydata2.max() - ydata2.min() + 1)  # TODO I'm not sure about off-by-oneness here... which would have an effect in small bin count (i.e. small numbers of seconds) plots
    print(bins)
    fig, ax = plt.subplots()

    ax.hist(ydata, bins=bins, color="#FF0000")
    ax.hist(ydata2, bins=bins, color="#0000FF")
    ax.legend()

    plt.xlabel(f"{event_name} events/second")
    plt.ylabel("number of tasks in bin")

    plt.savefig(output_filename)


plot_histo(waiting_data, running_data, "temp.png", "Work Queue WAITING/RUNNING")

# twoev.plot(parsl_task_invoked_to_returned, "parsl-task-durations", "parsl app invoked", "parsl app returned")


