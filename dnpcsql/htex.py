import os
import re

from dnpcsql.importerlib import local_key_to_span_uuid, logfile_time_to_unix, store_event

def import_htex(*,
    cursor,
    rundir: str):

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

                    htex_task_span_uuid = local_key_to_span_uuid(
                        cursor = cursor,
                        local_key = task_id,
                        namespace = htex_task_to_uuid,
                        span_type = 'parsl.executor.htex.task',
                        description = 'from interchange.log')

                    store_event(cursor=cursor,
                                span_uuid=htex_task_span_uuid,
                                event_time=event_time,
                                event_type='interchange_to_manager',
                                description='from interchange.log')

                m = re_interchange_removing_task.match(log_line)
                if m:
                    event_time = logfile_time_to_unix(m[1])
                    task_id = int(m[2])

                    htex_task_span_uuid = local_key_to_span_uuid(
                        cursor = cursor,
                        local_key = task_id,
                        namespace = htex_task_to_uuid,
                        span_type = 'parsl.executor.htex.task',
                        description = 'from interchange.log')

                    store_event(cursor=cursor,
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

                        htex_task_span_uuid = local_key_to_span_uuid(
                            cursor = cursor,
                            local_key = task_id,
                            namespace = htex_task_to_uuid,
                            span_type = 'parsl.executor.htex.task',
                            description = 'from interchange.log')

                        store_event(cursor=cursor,
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

                        htex_task_span_uuid = local_key_to_span_uuid(
                            cursor = cursor,
                            local_key = task_id,
                            namespace = htex_task_to_uuid,
                            span_type = 'parsl.executor.htex.task',
                            description = 'from inerchange.log')

                        store_event(cursor=cursor,
                                    span_uuid=htex_task_span_uuid,
                                    event_time=event_time,
                                    event_type='worker_received_task',
                                    description='from worker_*.log')

                    m = re_worker_completed_task.match(log_line)
                    if m:
                        event_time = logfile_time_to_unix(m[1])
                        task_id = int(m[2])

                        htex_task_span_uuid = local_key_to_span_uuid(
                            cursor = cursor,
                            local_key = task_id,
                            namespace = htex_task_to_uuid,
                            span_type = 'parsl.executor.htex.task',
                            description = 'from interchange.log')

                        store_event(cursor=cursor,
                                    span_uuid=htex_task_span_uuid,
                                    event_time=event_time,
                                    event_type='worker_completed_task',
                                    description='from worker_*.log'
                                   )

                    m = re_worker_all_finished_task.match(log_line)
                    if m:
                        event_time = logfile_time_to_unix(m[1])
                        task_id = int(m[2])

                        htex_task_span_uuid = local_key_to_span_uuid(
                            cursor = cursor,
                            local_key = task_id,
                            namespace = htex_task_to_uuid,
                            span_type = 'parsl.executor.htex.task',
                            description = 'from interchange.log')

                        store_event(cursor=cursor,
                                    span_uuid=htex_task_span_uuid,
                                    event_time=event_time,
                                    event_type='worker_all_finished_task',
                                    description='from worker_*.log')
    return htex_task_to_uuid
