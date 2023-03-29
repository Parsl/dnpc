import datetime
import os
import pickle
import re
import sqlite3
import time
import uuid

import dnpcsql.workqueue
from dnpcsql.htex import import_htex
from dnpcsql.importerlib import local_key_to_span_uuid, logfile_time_to_unix, store_event

from typing import Dict, Tuple, TypeVar

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

    if not os.path.exists(monitoring_db_name):
        print("monitoring.db does not exist - skipping monitoring.db import")
        return

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

        monitoring_task_to_uuid: Dict[int, str] = {}
        task_try_to_uuid = {}
        
        task_rows = list(monitoring_cursor.execute("SELECT task_id, task_time_invoked, task_time_returned FROM task WHERE run_id = ?", (run_id,)))
        for task_row in task_rows:
            task_id = task_row[0]
            print(f"  Importing task {task_id}")

            task_uuid = local_key_to_span_uuid(
                cursor = dnpc_cursor,
                local_key = int(task_id),
                namespace = monitoring_task_to_uuid,
                span_type = 'parsl.monitoring.task',
                description = "Task from parsl monitoring.db")

            dnpc_cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (run_id, task_uuid, task_id))

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

        print(f"(task,try)->uuid mappings are: {task_try_to_uuid}")

        # now we've imported a workflow from the monitoring DB
        # is there related stuff to import?

        rows = list(monitoring_cursor.execute("SELECT rundir FROM workflow WHERE run_id == ?", (run_id,)))
        assert len(rows) == 1

        rundir = rows[0][0]


        tracing_task_to_uuid = import_individual_rundir(dnpc_db=dnpc_db, cursor=dnpc_cursor, rundir=rundir, task_try_to_uuid=task_try_to_uuid)

        # TODO: it might not be appropriate to import this as a sub-activity of a monitoring.db workflow:
        # the rundirs also need importing when there is no monitoring.db
        # instead perhaps the rundirs should be imported alongside/in parallel to the monitorind db workflows,
        # and joined together as part of the "joiner" idea that i didn't explicitly start with.
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

def import_individual_rundir(*, dnpc_db, cursor, task_try_to_uuid: Dict[Tuple[int, int], str], rundir: str) -> Dict[str, str]:

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
            # -            logger.info("Task {} submitted to WorkQueue with id {}".format(task.id, wq_id))
            #  logger.info("Executor task {} submitted to Work Queue with Work Queue task id {}".format(task.id, wq_id))
            # this log line has two styles... style 1 is what is in master at time of writing, style 2 is from changes in desc branch to be clearer about the meaning of the word "task" as not referring to a parsl task
            re_wqe_to_wq_1 = re.compile('.* Task ([0-9]+) submitted to WorkQueue with id ([0-9]+).*')
            re_wqe_to_wq_2 = re.compile('.* Executor task ([0-9]+) submitted to Work Queue with Work Queue task id ([0-9]+).*')
            #  Executor task 0 submitted as Work Queue task 1 -- where is this coming from?
            re_wqe_to_wq_3 = re.compile('.* Executor task ([0-9]+) submitted as Work Queue task ([0-9]+).*')



            # 1668431173.633931 2022-11-14 05:06:13 WorkQueue-Submit-Process-60316 MainThread-140737354053440 parsl.executors.workqueue.executor:1007 _work_queue_submit_wait DEBUG: Completed WorkQueue task 3047, parsl executor task 3046
            # .* here before task because log message changed to add the word executor for clarity
            # but that isn't in master at time of writing.
            re_wq_compl = re.compile('([^ ]+) .* _work_queue_submit_wait .* Completed Work.*Queue task [0-9]+, parsl .*task ([0-9]+).*$')
            re_wq_compl2 = re.compile('([^ ]+) .* _work_queue_submit_wait .* Completed Work.*Queue task [0-9]+, executor task ([0-9]+).*$')

            wq_task_to_uuid = dnpcsql.workqueue.import_all(dnpc_db, wq_tl_filename)

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
            wqe_task_to_uuid: Dict[str, str] = {}
            parsl_log_filename = f"{rundir}/parsl.log"
            with open(parsl_log_filename, "r") as parsl_log:
                for parsl_log_line in parsl_log:

                    # this section binds an executor task to its containing
                    # parsl task.
                    m = re_parsl_log_bind_task.match(parsl_log_line)
                    if m and m[3] == executor_label:
                        task_try_id = (int(m[1]), int(m[2]))
                        wqe_id = m[4]
                        task_try_to_wqe[task_try_id] = wqe_id

                    # this section binds a work queue task to its containing
                    # executor task

                    # try first form of log line, and if that doesn't match
                    # fall through to the second style (in the form of an
                    # alternative / OR operator)
                    m = re_wqe_to_wq_1.match(parsl_log_line)
                    if not m:
                        m = re_wqe_to_wq_2.match(parsl_log_line)
                    if not m:
                        m = re_wqe_to_wq_3.match(parsl_log_line)

                    if m:
                        wqe_id = m[1]
                        wq_id = m[2]
                        wqe_to_wq[wqe_id] = wq_id

                    m = re_wq_compl.match(parsl_log_line)
                    if not m:
                        m = re_wq_compl2.match(parsl_log_line)

                    if m:
                        e_time = float(m[1])
                        wqe_id = m[2]
                        print("wq executor level event for wqe id {wqe_id}")
                        wqe_span_uuid = local_key_to_span_uuid(
                            cursor = cursor,
                            local_key = wqe_id,
                            namespace = wqe_task_to_uuid,
                            span_type = 'parsl.executors.workqueue.executor_task',
                            description = "WorkQueueExecutor task from parsl.log")

                        store_event(cursor=cursor,
                                    span_uuid=wqe_span_uuid,
                                    event_time=e_time,
                                    event_type='executor_completed',
                                    description='parsl.log entry for WQ Executor submit thread observing completion')

            print(f"task_try_to_wqe: {task_try_to_wqe}")
            print(f"wqe_to_wq: {wqe_to_wq}")

            assert len(wqe_to_wq) == len(task_try_to_wqe)
            assert len(task_try_to_wqe) == len(wq_task_to_uuid)

            print(f"len wq_task_to_uuid {len(wq_task_to_uuid)}")
            print(f"len(wqe_task_to_uuid {len(wqe_task_to_uuid)}")
            assert len(wq_task_to_uuid) == len(wqe_task_to_uuid)

            for (task_try_id, wqe_id) in task_try_to_wqe.items():
                print(f"pairing task_try_id {task_try_id} to Work Queue Executor task id {wqe_id}")
                try_span_uuid = task_try_to_uuid[task_try_id]
                wqe_id = task_try_to_wqe[task_try_id]
                wqe_task_span_uuid = wqe_task_to_uuid[wqe_id]
                cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (try_span_uuid, wqe_task_span_uuid, "parsl.executors.wq.task"))

                wq_id = wqe_to_wq[wqe_id]
                wq_span_uuid = wq_task_to_uuid[wq_id]
                print(f"Pairing Work Queue Executor task {wqe_task_span_uuid} to wq task span {wq_span_uuid}")

                # make a subspan relation that makes the wq task span
                # a subspan of the try

                cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (wqe_task_span_uuid, wq_span_uuid, "parsl.executors.wq.task"))
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
                    cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (wqe_task_log_span_uuid, 'parsl.wqexecutor.remote', 'parsl+wq executor'))

                    with open(function_log_filename, "r") as f:
                      for log_line in f.readlines():
                        print(log_line)
                        m = re_parsl_wq_task_log.match(log_line.strip())
                        if m:
                          print("Match")
                          event_time=float(m[1])
                          event_type=m[2]

                          # These events are extremely noisy and not so
                          # interesting so avoid importing them.
                          # TODO: perhaps avoiding these should be done
                          # at the query level?
                          if event_type.startswith("META_PATH "):
                            continue

                          store_event(cursor=cursor,
                                      span_uuid=wqe_task_log_span_uuid,
                                      event_time=event_time,
                                      event_type=event_type,
                                      description='parsl wq remote task log entry')

                    wq_id = wqe_to_wq[wqe_id]
                    wq_span_uuid = wq_task_to_uuid[wq_id]
                    cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (wq_span_uuid, wqe_task_log_span_uuid, "parsl.executors.wq.task.remote"))

            dnpc_db.commit()

        executor_label = "htex_Local"
        htex_task_to_uuid = import_htex(
            cursor=cursor,
            rundir=rundir)

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
                    cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (task_try_uuid, htex_task_uuid, "htex subtask"))

        dnpc_db.commit()

        tracing_task_to_uuid = import_parsl_tracing(
            cursor = cursor,
            rundir = rundir)
        dnpc_db.commit()

        return tracing_task_to_uuid


def import_parsl_tracing(*, cursor, rundir: str):
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

        tracing_span_uuids: Dict[Tuple[str, str], str] = {}

        for e in parsl_tracing['events']:
            event_time = e[0]
            event_name = e[1]
            span_type = e[2]
            span_id = e[3]

            k = (span_type, span_id)

            span_uuid = local_key_to_span_uuid(
                cursor = cursor,
                local_key = k,
                namespace = tracing_span_uuids,
                span_type = "parsl.tracing." + span_type,
                description = "imported from parsl_tracing")

            # can this be inferred later on in a binding stage
            # because the keys of tracing_span_uuids already
            # contain this information? it would split more nicely along
            # the importer A / importer B / binder A<->B modularisation
            # idea?
            if span_type == "TASK":
                tracing_task_id = span_id
                print(f"Found tracing TASK with ID {tracing_task_id}")
                tracing_task_to_uuid[tracing_task_id] = span_uuid

            store_event(cursor=cursor,
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

            super_uuid = local_key_to_span_uuid(
                cursor = cursor,
                local_key = super_k,
                namespace = tracing_span_uuids,
                span_type = "parsl.tracing." + super_type,
                description = "imported from parsl_tracing")

            sub_k = (sub_type, sub_id)

            sub_uuid = local_key_to_span_uuid(
                cursor = cursor,
                local_key = sub_k,
                namespace = tracing_span_uuids,
                span_type = "parsl.tracing." + sub_type,
                description = "imported from parsl_tracing")

            cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (super_uuid, sub_uuid, str((sub_type, sub_id))))

    return tracing_task_to_uuid

def db_time_to_unix(s: str):
    return datetime.datetime.fromisoformat(s).timestamp()
