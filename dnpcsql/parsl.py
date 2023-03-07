import datetime
import os
import pickle
import re
import sqlite3
import time
import uuid

import dnpcsql.workqueue
from dnpcsql.importerlib import store_event

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

        start_time = db_time_to_unix(row[1])

        store_event(cursor=dnpc_cursor,
                    span_uuid=run_id,
                    event_time=start_time,
                    event_type='began',
                    description='Start of workflow from parsl monitoring.db'
                   )

        if row[2]:  # non-null end time
            end_time = db_time_to_unix(row[2])

            store_event(cursor=dnpc_cursor,
                        span_uuid=run_id,
                        event_time=end_time,
                        event_type='completed',
                        description='End of workflow from parsl monitoring.db'
                       )

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

        monitoring_task_to_uuid = {}
        task_try_to_uuid = {}
        
        task_rows = list(monitoring_cursor.execute("SELECT task_id, task_time_invoked, task_time_returned FROM task WHERE run_id = ?", (run_id,)))
        for task_row in task_rows:
            print(f"  Importing task {task_row[0]}")
            task_uuid = str(uuid.uuid4())
            monitoring_task_to_uuid[int(task_row[0])] = task_uuid
            dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (task_uuid, 'parsl.monitoring.task', 'Task from parsl monitoring.db'))

            dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (run_id, task_uuid, task_row[0]))

            invoked_time = db_time_to_unix(task_row[1])

            store_event(cursor=dnpc_cursor,
                        span_uuid=task_uuid,
                        event_time=invoked_time,
                        event_type='invoked',
                        description='Task invoked in parsl monitoring.db'
                       )

            if task_row[2]:
                returned_time = db_time_to_unix(task_row[2])

                store_event(cursor=dnpc_cursor,
                            span_uuid=task_uuid,
                            event_time=returned_time,
                            event_type='returned',
                            description='Task returned in parsl monitoring.db'
                           )
            
            try_rows = list(monitoring_cursor.execute("SELECT try_id FROM try WHERE run_id = ? AND task_id = ?", (run_id, task_row[0])))
            for try_row in try_rows:
                print(f"    Importing try {try_row[0]}")
                try_uuid = str(uuid.uuid4())
                dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (try_uuid, 'parsl.monitoring.try', 'Try from parsl monitoring.db'))

                dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (task_uuid, try_uuid, try_row[0]))

                status_rows = list(monitoring_cursor.execute("SELECT task_status_name, timestamp FROM status WHERE run_id = ? AND task_id = ? AND try_id = ?", (run_id, task_row[0], try_row[0])))
                for status_row in status_rows:
                    print(f"      Importing status {status_row[0]} at {status_row[1]}")
                    status_time = db_time_to_unix(status_row[1])

                    store_event(cursor=dnpc_cursor,
                                span_uuid=try_uuid,
                                event_time=status_time,
                                event_type=status_row[0],
                                description='Status in parsl monitoring.db'
                               )

                # store (task,try) -> try span uuid mapping for use later
                task_try_to_uuid[(task_row[0], try_row[0])] = try_uuid

            # try table has timings, status table also has relevant timings... how to represent?

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


        re_parsl_log_bind_task = re.compile('.* Parsl task (.*) try (.*) launched on executor (.*) with executor id (.*)')

        print(f"Checking for Work Queue logs in rundir {rundir}")

        # TODO: this WorkQueue substring is hardcoded here to align with the
        # executor name used in the test suite. What should happen is that
        # each subdirectory is examined (or each executor-named subdirectory
        # from the database)

        executor_label = "WorkQueueExecutor"

        wq_tl_filename = f"{rundir}/{executor_label}/transaction_log"
        print(f"looking for: {wq_tl_filename}")
        if os.path.exists(wq_tl_filename):
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
                    m = re_parsl_log_bind_task.match(parsl_log_line)
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
                        e_time = m[1]
                        wq_id = m[2]
                        wq_span_uuid = wq_task_bindings[wq_id]

                        # TODO: this isn't part of the Work Queue level TASK so it should
                        # form part of the event span of the work queue executor task
                        # submission.

                        store_event(cursor=dnpc_cursor,
                                    span_uuid=wq_span_uuid,
                                    event_time=e_time,
                                    event_type='WQE_completed',
                                    description='parsl.log entry for WQ Executor submit thread observing completion')

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

                          store_event(cursor=dnpc_cursor,
                                      span_uuid=wqe_task_log_span_uuid,
                                      event_time=event_time,
                                      event_type=event_type,
                                      description='parsl wq remote task log entry')

                    wq_id = wqe_to_wq[wqe_id]
                    wq_span_uuid = wq_task_bindings[wq_id]
                    dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (wq_span_uuid, wqe_task_log_span_uuid, "parsl.executors.wq.task.remote"))

            dnpc_db.commit()

        # look for htex logs
        # right now using a hard-coded executor name
        # but could do something else like look in all directories
        # If funcx is going to be using parsl htex without a DFK around it,
        # which is likely, then this code should be able to import htex logs
        # without a surrounding parsl workflow/parsl tasks.
        # (but for example be able to bind to however htex will identify
        # it's submitted tasks one layer up - probably with a uuid)
        # That doesn't need to happen inside the parsl importer, but suggests
        # that the htex importer should be a separate module.

        htex_task_to_uuid = {}

        executor_label = "htex_Local"

        htex_interchange_filename = f"{rundir}/{executor_label}/interchange.log"
        print(f"looking for: {htex_interchange_filename}")
        if os.path.exists(htex_interchange_filename):
            # Interchange reports these two log lines which are relevant for
            # task status:
            # 2023-03-06 11:20:07.190 interchange:485 HTEX-Interchange(18277) MainThread process_tasks_to_send [DEBUG]  Sent tasks: [1] to manager b'4bf9bf8c1848'
            re_interchange_task_to_manager = re.compile('(.*) interchange:.* Sent tasks: \[(.*)\] to manager.*$')
            # 2023-03-06 11:20:08.040 interchange:533 HTEX-Interchange(18277) MainThread process_results_incoming [DEBUG]  Removing task 3 from manager record b'4bf9bf8c1848'
            re_interchange_removing_task = re.compile('(.*) interchange:.* Removing task ([0-9]+) .*$')
            with open(htex_interchange_filename, "r") as f:
                for log_line in f.readlines():
                    m = re_interchange_task_to_manager.match(log_line)
                    if m:
                        event_time = logfile_time_to_unix(m[1])
                        tasklist = m[2]
                        # TODO: this assumes that there's only one task in the task list
                        # which won't work except in serialised use cases - an exception
                        # will be raised here and you can read this comment and implement
                        # a suitable parse/for loop or change the interchange log
                        # message.
                        task_id = int(tasklist)

                        if task_id not in htex_task_to_uuid:
                            htex_task_span_uuid = str(uuid.uuid4())
                            htex_task_to_uuid[task_id] = htex_task_span_uuid
                            dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (htex_task_span_uuid, 'parsl.executor.htex.task', 'from interchange.log'))
                        else:
                            htex_task_span_uuid = htex_task_to_uuid[task_id]

                        store_event(cursor=dnpc_cursor,
                                    span_uuid=htex_task_span_uuid,
                                    event_time=event_time,
                                    event_type='interchange_to_manager',
                                    description='from interchange.log')

                    m = re_interchange_removing_task.match(log_line)
                    if m:
                        event_time = logfile_time_to_unix(m[1])
                        task_id = int(m[2])

                        if task_id not in htex_task_to_uuid:
                            htex_task_span_uuid = str(uuid.uuid4())
                            htex_task_to_uuid[task_id] = htex_task_span_uuid
                            dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (htex_task_span_uuid, 'parsl.executor.htex.task', 'from interchange.log'))
                        else:
                            htex_task_span_uuid = htex_task_to_uuid[task_id]

                        store_event(cursor=dnpc_cursor,
                                    span_uuid=htex_task_span_uuid,
                                    event_time=event_time,
                                    event_type='interchange_removing_task',
                                    description='from interchange.log')

        # next, manager logs
        # 2023-03-06 11:20:07.190 parsl:304 18294 Task-Puller [DEBUG]  Got executor tasks: [1], cumulative count of tasks: 1
        re_manager_got_tasks = re.compile('(.*) parsl:.* Got executor tasks: \[(.+)\].*$')
        # manager does not log the identity of task results - because it never unpacks them? not sure why that's different than the task send path?

        # manager logs appear under a block ID then a manager ID in the path,
        # e.g. runinfo/000/htex_Local/block-0/4bf9bf8c1848/manager.log

        # so find all of these and then run manager and worker log processing
        # for each manager directory.

        manager_dirs = [d for (d,df,ff) in os.walk(f"{rundir}/{executor_label}") if "manager.log" in ff]

        for manager_dir in manager_dirs:
            print(f"Processing manager directory {manager_dir}")

            manager_filename = f"{manager_dir}/manager.log"
            print(f"looking for: {manager_filename}")
            if os.path.exists(manager_filename):
                with open(manager_filename, "r") as f:
                    for log_line in f.readlines():
                        m = re_manager_got_tasks.match(log_line)
                        if m:
                            event_time = logfile_time_to_unix(m[1])
                            task_id = int(m[2])
                            if task_id not in htex_task_to_uuid:
                                htex_task_span_uuid = str(uuid.uuid4())
                                htex_task_to_uuid[task_id] = htex_task_span_uuid
                                dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (htex_task_span_uuid, 'parsl.executor.htex.task', 'from interchange.log'))
                            else:
                                htex_task_span_uuid = htex_task_to_uuid[task_id]

                            store_event(cursor=dnpc_cursor,
                                        span_uuid=htex_task_span_uuid,
                                        event_time=event_time,
                                        event_type='manager_got_task',
                                        description='from manager.log')

            else:
                raise RuntimeError("manager log was not found in manager directory")

            # next, worker logs
            # 2023-03-06 11:20:17.249 worker_log:597 18304 MainThread [INFO]  Received executor task 41
            re_worker_received_task = re.compile('(.*) worker_log:.* Received executor task ([^ ]+).*$')
            # 2023-03-06 11:20:17.282 worker_log:615 18304 MainThread [INFO]  Completed executor task 41
            re_worker_completed_task = re.compile('(.*) worker_log:.* Completed executor task ([^ ]+).*$')
            # 2023-03-06 11:20:17.282 worker_log:626 18304 MainThread [INFO]  All processing finished for executor task 41
            re_worker_all_finished_task = re.compile('(.*) worker_log:.* All processing finished for executor task ([^ ]+).*$')

            # worker log files are in the manager directory and are named like this:
            # runinfo/000/htex_Local/block-0/4bf9bf8c1848/worker_6.log

            worker_logs = [f for f in os.listdir(manager_dir) if f.startswith("worker_")]

            for worker_filename in worker_logs:
                with open(f"{manager_dir}/{worker_filename}", "r") as f:
                    for log_line in f.readlines():
                        m = re_worker_received_task.match(log_line)
                        if m:
                            event_time = logfile_time_to_unix(m[1])
                            task_id = int(m[2])
                            if task_id not in htex_task_to_uuid:
                                htex_task_span_uuid = str(uuid.uuid4())
                                htex_task_to_uuid[task_id] = htex_task_span_uuid
                                dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (htex_task_span_uuid, 'parsl.executor.htex.task', 'from interchange.log'))
                            else:
                                htex_task_span_uuid = htex_task_to_uuid[task_id]

                            store_event(cursor=dnpc_cursor,
                                        span_uuid=htex_task_span_uuid,
                                        event_time=event_time,
                                        event_type='worker_received_task',
                                        description='from worker_*.log')

                        m = re_worker_completed_task.match(log_line)
                        if m:
                            event_time = logfile_time_to_unix(m[1])
                            task_id = int(m[2])
                            if task_id not in htex_task_to_uuid:
                                htex_task_span_uuid = str(uuid.uuid4())
                                htex_task_to_uuid[task_id] = htex_task_span_uuid
                                dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (htex_task_span_uuid, 'parsl.executor.htex.task', 'from interchange.log'))
                            else:
                                htex_task_span_uuid = htex_task_to_uuid[task_id]


                            store_event(cursor=dnpc_cursor,
                                        span_uuid=htex_task_span_uuid,
                                        event_time=event_time,
                                        event_type='worker_completed_task',
                                        description='from worker_*.log'
                                       )

                        m = re_worker_all_finished_task.match(log_line)
                        if m:
                            event_time = logfile_time_to_unix(m[1])
                            task_id = int(m[2])
                            if task_id not in htex_task_to_uuid:
                                htex_task_span_uuid = str(uuid.uuid4())
                                htex_task_to_uuid[task_id] = htex_task_span_uuid
                                dnpc_cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (htex_task_span_uuid, 'parsl.executor.htex.task', 'from interchange.log'))
                            else:
                                htex_task_span_uuid = htex_task_to_uuid[task_id]

                            store_event(cursor=dnpc_cursor,
                                        span_uuid=htex_task_span_uuid,
                                        event_time=event_time,
                                        event_type='worker_all_finished_task',
                                        description='from worker_*.log')

        # now bind htex tasks to parsl tries
        # this code is related to code in the work queue importing code
        # that does the same "executor level task ID to parsl level
        # try ID", but WQ has an extra layer of IDs beyond the executor
        # task ID that htex does not.

        parsl_log_filename = f"{rundir}/parsl.log"
        with open(parsl_log_filename, "r") as parsl_log:
            for parsl_log_line in parsl_log:
                m = re_parsl_log_bind_task.match(parsl_log_line)
                if m and m[3] == executor_label:
                    task_try_id = (int(m[1]), int(m[2]))
                    htex_task_id = int(m[4])

                    task_try_uuid = task_try_to_uuid[task_try_id]
                    htex_task_uuid = htex_task_to_uuid[htex_task_id]
                    dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (task_try_uuid, htex_task_uuid, "htex subtask"))

        dnpc_db.commit()

        # Now import pickled event stats from parsl_tracing.pickle which is
        # a DESC-branch specific development.
        # Right now there isn't enough info to tie such a pickle file into
        # particular workflow: that's because there is a conflation between
        # a Python process and a DFK (aka workflow) instance, with a lot
        # of parsl code not being aware of how it is associated with a
        # particular DFK.
        # so this code will have to make some assumptions.

        tracing_task_to_uuid = {}

        parsl_tracing_filename=f"{rundir}/parsl_tracing.pickle"
        if os.path.exists(parsl_tracing_filename):
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

                    # TODO: also do this for tasks added at bind level...
                    # eg by factoring out this block.
                    if span_type == "TASK":
                        tracing_task_id = span_id
                        print(f"Found tracing TASK with ID {tracing_task_id}")
                        tracing_task_to_uuid[tracing_task_id] = span_uuid
                else:
                    span_uuid = tracing_span_uuids[k]
   
                store_event(cursor=dnpc_cursor,
                            span_uuid=span_uuid,
                            event_time=event_time,
                            event_type=event_name,
                            description='imported from parsl_tracing')
 
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

        # now tie together facets of the same entity from tracing and monitoring:
        # tasks
        # tries
        # for a top level tasks-based analysis, it probably suffices to only tie
        # together at the task level, but if working with tries individually,
        # then it's probably interesting to consider tying together tries too,
        # so that the relationship is directly expressed in the database rather
        # than via try id numbers.
        # I wonder if there's a more modular identifier based way of doing this,
        # rather than collecting in-memory references? so that a SELECT->INSERT
        # could make the entity links (i.e. the DB would already contain the
        # relevant data in a different form?)

        known_task_ids = set(list(tracing_task_to_uuid.keys()) + list(monitoring_task_to_uuid.keys()))
        print(f"There are {len(known_task_ids)} known tasks, between monitoring and tracing") 
        for task_id in known_task_ids:
            if task_id in tracing_task_to_uuid and task_id in monitoring_task_to_uuid:
                print(f"joining spans for task {task_id}")
                dnpc_cursor.execute("INSERT INTO facet (left_uuid, right_uuid, note) VALUES (?, ?, ?)", (tracing_task_to_uuid[task_id], monitoring_task_to_uuid[task_id], "joined by importer"))

        dnpc_db.commit()

def db_time_to_unix(s: str):
    return datetime.datetime.fromisoformat(s).timestamp()

def logfile_time_to_unix(s: str) -> float:
    """Converts a parsl logfile timestamp like 2023-03-06 11:20:17.282
    to a unix time"""
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").timestamp()
