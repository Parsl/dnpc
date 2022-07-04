import sqlite3
import uuid

# There are multiple parsl data sources.
# The big ones are:
# - monitoring.db
# - */parsl.log
#
# but there are also, for example, htex
# interchange and worker logs, and workqueue
# logs. (anything else?)
#
# It would be nice to not have to assume
# any of these definitely exists - but some of
# the tying-together data exists only in
# the monitoring db and parsl.log...
#
# There is also going to be some kind of consistency
# requirement on what lives in rundir - eg don't delete
# all the runs but leave monitoring.db in place, and
# end up with a second run in runinfo/000/ ?
# (although parsl.log hopefully has a run ID in there
# that corresponds with the monitoring db - and that
# probably works better than an absolute path? That gives
# a different consistency requirement of not using
# multiple DFKs in a single parsl.log? which is actually
# perhaps an LSST/DESC requirement)

def import_all(db: sqlite3.Connection):
    print("importing from parsl")

    import_monitoring_db(db, "/home/benc/parsl/src/parsl/runinfo/monitoring.db")

    print("done importing from parsl")

def import_monitoring_db(dnpc_db, monitoring_db_name):

    monitoring_db = sqlite3.connect(monitoring_db_name,
                                    detect_types=sqlite3.PARSE_DECLTYPES |
                                    sqlite3.PARSE_COLNAMES)

    monitoring_cursor = monitoring_db.cursor()
    dnpc_cursor = dnpc_db.cursor()

    rows = list(monitoring_cursor.execute("SELECT run_id, time_began, time_completed FROM workflow"))

    for row in rows:
        run_id = row[0]
        print(f"Found workflow run_id {run_id} in monitoring database")

        # this should result in:
        # a span for the workflow, with up to two events, the beginning and
        # the end.  The end time is optional: a crashed or still running
        # workflow will not have that (and I'm unclear how to tell the
        # difference between a gone-away workflow and a workflow that
        # hasn't reported any activity for a while)

        # this will trust UUID generation in parsl enough that the run_id
        # can be used to name the workflow span.

        dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (run_id, 'parsl.workflow', 'Workflow from parsl monitoring.db'))

        start_uuid = str(uuid.uuid4())
        dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (start_uuid, run_id, row[1], 'began', 'Start of workflow from parsl monitoring.db'))

        if row[2]:  # non-null end time
            end_uuid = str(uuid.uuid4())
            dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (end_uuid, run_id, row[2], 'completed', 'End of workflow from parsl monitoring.db'))

        dnpc_db.commit()

        # under a workflow there are multiple hierarchies:
        # task -> try
        #    and there are multiple state transition representations here:
        #    the status table, and the several task/try table timestamp columns
        #    What's the best way to reconcile this?
        # executor -> task (-> try)
        # executor -> block -> try  # note that tasks aren't assigned to a block -- tries are.

        # the one most obviously represented by the key structure of the parsl
        # monitoring db is task->try

        
        task_rows = list(monitoring_cursor.execute("SELECT task_id, task_time_invoked, task_time_returned FROM task WHERE run_id = ?", (run_id,)))
        for task_row in task_rows:
            print(f"Importing task {task_row[0]}")
            task_uuid = str(uuid.uuid4())
            dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (task_uuid, 'parsl.task', 'Task from parsl monitoring.db'))

            dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (run_id, task_uuid, task_row[0]))

            # TODO add task_time_{invoked,returned} as events
            invoked_uuid = str(uuid.uuid4())
            dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (invoked_uuid, task_uuid, row[1], 'invoked', 'Task invoked in parsl monitoring.db'))

            returned_uuid = str(uuid.uuid4())
            dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (returned_uuid, task_uuid, row[2], 'returned', 'Task returned in parsl monitoring.db'))
            # TODO: import tries, and make events for each try from the status table

            dnpc_db.commit()
