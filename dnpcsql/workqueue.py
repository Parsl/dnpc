import re
import sqlite3
import uuid

def import_all(db: sqlite3.Connection, transaction_log_path):
    """Imports tasks from transaction_log and returns a dict that maps
    from work queue task numbers to the relevant task spans, with the
    intention that this be used by integrating pieces to tie wq tasks
    into containing spans.
    """
    print("importing from work queue")

    # TODO: how should we discover these paths?
    # Some outside entity (eg the parsl monitoring DB code) knows where this
    # file lives and how it relates to parsl - so probably this should be
    # driven by the parsl importer in that case. While also being suitable for
    # importing work queue abstracted from parsl.

    # Each task becomes a span (without being a subspan of anything at the
    # moment, but this will need to happen somehow to eg tie into the
    # relevant parsl-level task spans)

    cre = re.compile('([0-9]+) [0-9]+ TASK ([0-9]+) ([^ ]+) .*')

    task_to_span_map = {}

    cursor = db.cursor()

    with open(transaction_log_path, "r") as logfile:
        for line in logfile:
            print(line)
            m = cre.match(line)
            if m:
                print(m)
                print(m[1])
                print(m[2])
                print(m[3])

                wq_task_id = m[2]
                if wq_task_id not in task_to_span_map:
                    span_id = str(uuid.uuid4())
                    task_to_span_map[wq_task_id] = span_id
                    print(f"New task {wq_task_id} - create span {span_id}")
                    cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (span_id, 'workqueue.task', 'Work Queue TASK from transaction_log'))

 
                else:
                    span_id = task_to_span_map[wq_task_id]
                    print(f"Existing task {wq_task_id} with span {span_id}")

                event_uuid = str(uuid.uuid4())
                cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (event_uuid, span_id, m[1], m[3], 'Event from transaction_log'))
 
    db.commit() 
    print("done importing from work_queue")
    return task_to_span_map
