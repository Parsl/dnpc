import datetime
import os
import pickle
import re
import sqlite3
import time
import uuid

import dnpcsql.workqueue

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

def import_all(db: sqlite3.Connection, runinfo: str):
    print("importing from parsl")

    import_monitoring_db(db, f"{runinfo}/monitoring.db")

    print("done importing from parsl")

def import_monitoring_db(dnpc_db, monitoring_db_name):

    print(f"importing from monitoring db: {monitoring_db_name}")
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

        dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (run_id, 'parsl.monitoring.workflow', 'Workflow from parsl monitoring.db'))

        start_uuid = str(uuid.uuid4())
        start_time = db_time_to_unix(row[1])
        dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (start_uuid, run_id, start_time, 'began', 'Start of workflow from parsl monitoring.db'))

        if row[2]:  # non-null end time
            end_uuid = str(uuid.uuid4())
            end_time = db_time_to_unix(row[2])
            dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (end_uuid, run_id, end_time, 'completed', 'End of workflow from parsl monitoring.db'))

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

        task_try_to_uuid = {}
        
        task_rows = list(monitoring_cursor.execute("SELECT task_id, task_time_invoked, task_time_returned FROM task WHERE run_id = ?", (run_id,)))
        for task_row in task_rows:
            print(f"  Importing task {task_row[0]}")
            task_uuid = str(uuid.uuid4())
            dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (task_uuid, 'parsl.monitoring.task', 'Task from parsl monitoring.db'))

            dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (run_id, task_uuid, task_row[0]))

            invoked_uuid = str(uuid.uuid4())
            invoked_time = db_time_to_unix(task_row[1])
            dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (invoked_uuid, task_uuid, invoked_time, 'invoked', 'Task invoked in parsl monitoring.db'))

            if task_row[2]:
                returned_uuid = str(uuid.uuid4())
                returned_time = db_time_to_unix(task_row[2])
                dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (returned_uuid, task_uuid, returned_time, 'returned', 'Task returned in parsl monitoring.db'))
            
            try_rows = list(monitoring_cursor.execute("SELECT try_id FROM try WHERE run_id = ? AND task_id = ?", (run_id, task_row[0])))
            for try_row in try_rows:
                print(f"    Importing try {try_row[0]}")
                # print(f"* PRE INSERT SPAN {time.time()}")
                try_uuid = str(uuid.uuid4())
                dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (try_uuid, 'parsl.monitoring.try', 'Try from parsl monitoring.db'))

                # print(f"* PRE INSERT SUBSPAN {time.time()}")
                dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (task_uuid, try_uuid, try_row[0]))

                # print(f"* PRE SELECT STATUS {time.time()}")
                status_rows = list(monitoring_cursor.execute("SELECT task_status_name, timestamp FROM status WHERE run_id = ? AND task_id = ? AND try_id = ?", (run_id, task_row[0], try_row[0])))
                for status_row in status_rows:
                    print(f"      Importing status {status_row[0]} at {status_row[1]}")
                    status_uuid = str(uuid.uuid4())
                    status_time = db_time_to_unix(status_row[1])
                    # print(f"* PRE INSERT EVENT {time.time()}")
                    dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (status_uuid, try_uuid, status_time, status_row[0], 'Status in parsl monitoring.db'))

                # store (task,try) -> try span uuid mapping for use later
                task_try_to_uuid[(task_row[0], try_row[0])] = try_uuid

            # try table has timings, status table also has relevant timings... how to represent?

            # print(f"* PRE COMMIT {time.time()}")
            # dnpc_db.commit()
            # print(f"* POST COMMIT {time.time()}")

        # trying out commit at end of everything for potentially large speedup
        dnpc_db.commit()
        # now we've imported a workflow from the monitoring DB
        # is there related stuff to import?
        # For now, that is just work queue task information, but this would
        # also be the place to import 
        # How can we tell when an executor has workqueue stuff to import?
        # Let's assume that if there is a nnn/*/transaction_log file, then
        # it should be imported.

        rows = list(monitoring_cursor.execute("SELECT rundir FROM workflow WHERE run_id == ?", (run_id,)))
        assert len(rows) == 1

        rundir = rows[0][0]

        print(f"(task,try)->uuid mappings are: {task_try_to_uuid}")

        print(f"Checking for Work Queue logs in rundir {rundir}")

        # TODO: this WorkQueue substring is hardcoded here to align with the
        # executor name used in the test suite. What should happen is that
        # each subdirectory is examined (or each executor-named subdirectory
        # from the database)

        executor_label = "WorkQueueExecutor"

        wq_tl_filename = f"{rundir}/{executor_label}/transaction_log"
        print(f"looking for: {wq_tl_filename}")
        if os.path.exists(wq_tl_filename):
            re1 = re.compile('.* Parsl task (.*) try (.*) launched on executor (.*) with executor id (.*)')
            # 140737354053440 parsl.executors.workqueue.executor:994 _work_queue_submit_wait INFO: Executor task 20362 submitted to Work Queue with Work Queue task id 20363
            re_wqe_to_wq = re.compile('.* Executor task ([0-9]+) submitted to Work Queue with Work Queue task id ([0-9]+).*')

            # 1668431173.633931 2022-11-14 05:06:13 WorkQueue-Submit-Process-60316 MainThread-140737354053440 parsl.executors.workqueue.executor:1007 _work_queue_submit_wait DEBUG: Completed WorkQueue task 3047, parsl executor task 3046
            re_wq_compl = re.compile('([^ ]+) .* _work_queue_submit_wait .* Completed WorkQueue task ([0-9]+),.*$')

            wq_task_bindings = dnpcsql.workqueue.import_all(dnpc_db, wq_tl_filename)

            # now (via the wq executor task id) bind these together.
            # perhaps it would simplify things to make the in-parsl
            # presentation of these three IDs nicer, for example, by placing
            # the end work queue TASK id into the monitoring database,
            # but this immediate work is to deal with what is already there.

            # I'll also need something to map parsl task/try IDs to span IDs
            # to specify the other end of the subspan relationship - eg by
            # collecting that information as we go along above.

            task_try_to_wqe = {}
            wqe_to_wq = {}
            parsl_log_filename = f"{rundir}/parsl.log"
            with open(parsl_log_filename, "r") as parsl_log:
                for parsl_log_line in parsl_log:
                    # print(parsl_log_line)
                    m = re1.match(parsl_log_line)
                    if m and m[3] == executor_label:
                        task_try_id = (int(m[1]), int(m[2]))
                        wqe_id = m[4]
                        task_try_to_wqe[task_try_id] = wqe_id
                    m = re_wqe_to_wq.match(parsl_log_line)
                    if m:
                        wqe_id = m[1]
                        wq_id = m[2]
                        wqe_to_wq[wqe_id] = wq_id
                    m = re_wq_compl.match(parsl_log_line)
                    if m:
                        # print("adding completion event to wq")
                        e_time = m[1]
                        wq_id = m[2]
                        wq_span_uuid = wq_task_bindings[wq_id]
                        e_uuid = str(uuid.uuid4())
                        # TODO: this isn't part of the Work Queue level TASK so it should
                        # form part of the event span of the work queue executor task
                        # submission.
                        dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (e_uuid, wq_span_uuid, e_time, 'WQE_completed', 'parsl.log entry for WQ Executor submit thread observing completion'))
                        

            print(f"task_try_to_wqe: {task_try_to_wqe}")
            print(f"wqe_to_wq: {wqe_to_wq}")

            for (task_try_id, wqe_id) in task_try_to_wqe.items():
                print(f"pairing task_try_id {task_try_id} to Work Queue Executor task id {wqe_id}")
                try_span_uuid = task_try_to_uuid[task_try_id]
                wqe_id = task_try_to_wqe[task_try_id]
                wq_id = wqe_to_wq[wqe_id]
                wq_span_uuid = wq_task_bindings[wq_id]
                print(f"map try span {try_span_uuid} to wq task span {wq_span_uuid}")

                # make a subspan relation that makes the wq task span
                # a subspan of the try

                dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (try_span_uuid, wq_span_uuid, "parsl.executors.wq.task"))
            dnpc_db.commit()

            # 1677161346.713548 META_PATH parsl.tests.test_regression.test_2555
            re_parsl_wq_task_log = re.compile('^([0-9.]+) (.*)$')
            # now look at importing parsl wq inside-executor loading logs
            for wqe_id in wqe_to_wq.keys():
                print(f"Looking for inside-executor loading logs for executor task ID {wqe_id}")
                task_dir = "{:04d}".format(int(wqe_id))

                function_log_filename = f"{rundir}/{executor_label}/function_data/{task_dir}/log"
                print(f"Filename: {function_log_filename}")
                if os.path.exists(function_log_filename):
                    print("WQ task log file exists")
                    wqe_task_log_span_uuid = str(uuid.uuid4())
                    dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (wqe_task_log_span_uuid, 'parsl.wqexecutor.remote', 'parsl+wq executor'))

                    with open(function_log_filename, "r") as f:
                      for log_line in f.readlines():
                        print(log_line)
                        m = re_parsl_wq_task_log.match(log_line.strip())
                        if m:
                          print("Match")
                          event_time=m[1]
                          event_type=m[2]
                          if event_type.startswith("META_PATH "):
                            continue
                          event_uuid = str(uuid.uuid4())
                          dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (event_uuid, wqe_task_log_span_uuid, event_time, event_type, 'parsl wq remote task log entry'))

                    wq_id = wqe_to_wq[wqe_id]
                    wq_span_uuid = wq_task_bindings[wq_id]
                    dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (wq_span_uuid, wqe_task_log_span_uuid, "parsl.executors.wq.task.remote"))

            dnpc_db.commit()

            # Now import pickled event stats from parsl_tracing.pickle which is
            # a DESC-branch specific development.
            # Right now there isn't enough info to tie such a pickle file into
            # particular workflow: that's because there is a conflation between
            # a Python process and a DFK (aka workflow) instance, with a lot
            # of parsl code not being aware of how it is associated with a
            # particular DFK.
            # so this code will have to make some assumptions.

            parsl_tracing_filename=f"{rundir}/parsl_tracing.pickle"
            with open(parsl_tracing_filename, "rb") as f:
                parsl_tracing = pickle.load(f)

            assert 'events' in parsl_tracing
            assert 'binds' in parsl_tracing

            # parsl_tracing.pickle contains both events and binds between
            # spans. The existence of spans is implict, by being mentioned
            # either in an event or a bind, so a span cannot exist in
            # isolation with neither events nor binds.

            tracing_span_uuids = {}

            for e in parsl_tracing['events']:
                event_time = e[0]
                event_name = e[1]
                span_type = e[2]
                span_id = e[3]

                k = (span_type, span_id)

                # this bit handles the implicitness of span existence in
                # parsl_tracing
                if k not in tracing_span_uuids:
                    span_uuid = str(uuid.uuid4())
                    tracing_span_uuids[k] = span_uuid
                    db_span_type = "parsl.tracing." + span_type
                    dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (span_uuid, db_span_type, 'imported from parsl_tracing'))
                else:
                    span_uuid = tracing_span_uuids[k]

                event_uuid = str(uuid.uuid4())
                dnpc_cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (event_uuid, span_uuid, event_time, event_type, 'imported from parsl_tracing'))

            for b in parsl_tracing['binds']:
                super_type = b[0]
                super_id = b[1]
                sub_type = b[2]
                sub_id = b[3]

                super_k = (super_type, super_id)
                if super_k not in tracing_span_uuids:
                    super_uuid = str(uuid.uuid4())
                    tracing_span_uuids[super_k] = super_uuid
                    db_span_type = "parsl.tracing." + super_type
                    dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (super_uuid, db_span_type, 'imported from parsl_tracing event'))
                else:
                    super_uuid = tracing_span_uuids[super_k]


                sub_k = (sub_type, sub_id)
                if sub_k not in tracing_span_uuids:
                    sub_uuid = str(uuid.uuid4())
                    tracing_span_uuids[super_k] = sub_uuid
                    db_span_type = "parsl.tracing." + sub_type
                    dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (sub_uuid, db_span_type, 'imported from parsl_tracing bind'))
                else:
                    sub_uuid = tracing_span_uuids[sub_k]


                dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (super_uuid, sub_uuid, str((sub_type, sub_id))))

            dnpc_db.commit()


def db_time_to_unix(s: str):
    return datetime.datetime.fromisoformat(s).timestamp()

