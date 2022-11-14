import matplotlib.pyplot as plt
import numpy as np
import sqlalchemy
import sqlite3

if __name__ == "__main__":
    print("task end latency analysis on cori, november 2022")

    db_name = "./dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                     detect_types=sqlite3.PARSE_DECLTYPES |
                     sqlite3.PARSE_COLNAMES)

    cursor = db.cursor()

    query = """
    SELECT start_event.time, start_event.time - end_event.time
    FROM event as start_event,
         event as end_event,
         span as start_span,
         span as end_span,
         subspan
    WHERE start_event.span_uuid = start_span.uuid
      AND start_event.type = "exec_done"
      AND start_span.type = "parsl.try"
      AND start_span.uuid = subspan.superspan_uuid
      AND end_span.uuid = subspan.subspan_uuid
      AND end_span.type = "workqueue.task"
      AND end_event.type = "WAITING_RETRIEVAL"
      AND end_event.span_uuid = end_span.uuid
    ;
    """
   # select julianday(status.timestamp) * 86400, (julianday(task_time_returned) - julianday(status.timestamp)) * 86400 as D from task inner join status on status.task_id = task.task_id where status.task_status_name = "running_ended";
    rows = list(cursor.execute(query))
    assert len(rows) == 32000

    print(f"there are {len(rows)} relevant status transitions in the db")

    xdata = np.array([float(x) for (x,y) in rows])
    xdata = xdata - xdata.min()

    ydata = np.array([float(y) for (x,y) in rows])

    fig, ax = plt.subplots()

    # ax.hist(durations_running_ended_exec_done, bins=100, color="#0000FF")
    ax.scatter(x=xdata, y=ydata, s=1)

    plt.xlabel("clock time of end event")
    plt.ylabel("s taken to process")

    plt.savefig("WAITING_RETRIEVAL_to_exec_done.png")

