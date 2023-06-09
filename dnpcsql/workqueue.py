import re
import sqlite3
import uuid

from typing import Dict

from dnpcsql.importerlib import local_key_to_span_uuid, store_event

def import_all(db: sqlite3.Connection, transaction_log_path) -> Dict[str, str]:
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

    task_re = re.compile('([0-9]+) [0-9]+ TASK ([0-9]+) ([^ ]+) ([^ ]+).*')

    # time manager_pid TRANSFER (INPUT|OUTPUT) taskid cache_flag sizeinmb walltime filename
    transfer_re = re.compile('([0-9]+) [0-9]+ TRANSFER ([^ ]+) ([0-9]+) ([^ ]+) .*')

    task_to_span_map: Dict[str, str] = {}

    cursor = db.cursor()

    worker_address_to_span_map: Dict[str, str] = {}

    with open(transaction_log_path, "r") as logfile:
        for line in logfile:
            print(line)
            m = task_re.match(line)
            if m:
                print(m)
                print(m[1])
                print(m[2])
                print(m[3])

                wq_task_id = m[2]

                task_span_uuid = local_key_to_span_uuid(
                    cursor = cursor,
                    local_key = wq_task_id,
                    namespace = task_to_span_map,
                    span_type = 'workqueue.task',
                    description = 'Work Queue TASK from transaction_log')
 
                unix_time = float(m[1]) / 1000000.0

                store_event(cursor=cursor,
                            span_uuid=task_span_uuid,
                            event_time=unix_time,
                            event_type=m[3],
                            description='Event from transaction_log'
                           )

                if m[3] in ["RUNNING", "WAITING_RETRIEVAL"]:  # we can capture worker ID
                    worker_address = m[4]
                    worker_span_uuid = local_key_to_span_uuid(
                        cursor = cursor,
                        local_key = worker_address,
                        namespace = worker_address_to_span_map,
                        span_type = 'workqueue.worker',
                        description = 'Work Queue WORKER from transaction_log')

                    # TODO: don't need to do this on both RUNNING and WAITING_RETRIEVAL...
                    cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (worker_span_uuid, task_span_uuid, wq_task_id))

            m = transfer_re.match(line)
            if m:
                wq_task_id = m[3]

                span_id = local_key_to_span_uuid(
                    cursor = cursor,
                    local_key = wq_task_id,
                    namespace = task_to_span_map,
                    span_type = 'workqueue.task',
                    description = 'Work Queue TASK from transaction_log')
 
                unix_time = float(m[1]) / 1000000.0

                store_event(cursor=cursor,
                            span_uuid=span_id,
                            event_time=unix_time,
                            event_type="TRANSFER_"+m[2],
                            description='Event from transaction_log'
                           )




    db.commit() 
    print("done importing from work_queue")
    return task_to_span_map
