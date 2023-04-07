import re
import sqlite3
import uuid

# an importer for funcx synthetic workflow


log_filename = "funcxsynth/synthetic.log"

db_name = "dnpc.sqlite3"
db = sqlite3.connect(db_name,
                     detect_types=sqlite3.PARSE_DECLTYPES |
                     sqlite3.PARSE_COLNAMES)

cursor = db.cursor()

synth_id_to_span_uuid_remote = {}
synth_id_to_span_uuid_submit = {}


with open(log_filename, "r") as log:

    re_remote = re.compile("^[0-9.]+ .* FUNCREMOTE ([0-9]+) ([^ ]+) ([0-9.]+).*$")

    re_submit = re.compile("^([0-9.]+) .* FUNC ([0-9]+) ([^ \n]+).*$")

    for line in log:
        print(f"* {line}")

        # import lines that look like they are reports from the
        # on-worker timings passed back through application
        # specific channels:

        # 1657213012.870369 2022-07-07 18:56:52,870 MainProcess-35539 MainThread-140429489739584 synthetic_client:53 <module> INFO: FUNCREMOTE 3099 start 1657213011.3501174
        # 1657213012.870509 2022-07-07 18:56:52,870 MainProcess-35539 MainThread-140429489739584 synthetic_client:54 <module> INFO: FUNCREMOTE 3099 end 1657213012.7515485

        m = re_remote.match(line)
        if m:
            print("MATCH - remote")
            synth_task_id = m[1]
            synth_event_type = m[2]
            synth_time = float(m[3])

            print(f"id={synth_task_id} event={synth_event_type} time={synth_time}")

            if synth_task_id not in synth_id_to_span_uuid_remote:
                print("Need to create a new span")
                span_uuid = str(uuid.uuid4())
                synth_id_to_span_uuid_remote[synth_task_id] = span_uuid

                cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (span_uuid, 'synthetic.task.worker', 'Synthetic app view of a task, from the worker'))

            else:
                span_uuid = synth_id_to_span_uuid_remote[synth_task_id]

            event_uuid = str(uuid.uuid4())
            cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (event_uuid, span_uuid, synth_time, synth_event_type, 'progress event from remote synthetic workload task'))

        # import lines that refer to submit-side progress
        # 1657212868.582289 2022-07-07 18:54:28,582 MainProcess-35539 MainThread-140429489739584 synthetic_client:43 <module> INFO: FUNC 3000 submit
        m = re_submit.match(line)
        if m:
            print("MATCH - submit")
            synth_task_id = m[2]
            synth_event_type = m[3]
            synth_time = float(m[1])

            if synth_task_id not in synth_id_to_span_uuid_submit:
                print("Need to create a new span")
                span_uuid = str(uuid.uuid4())
                synth_id_to_span_uuid_submit[synth_task_id] = span_uuid

                cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (span_uuid, 'synthetic.task.submit', 'Synthetic app view of a task, from the submit side'))

            else:
                span_uuid = synth_id_to_span_uuid_submit[synth_task_id]

            event_uuid = str(uuid.uuid4())
            cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (event_uuid, span_uuid, synth_time, synth_event_type, 'progress event from submitting synthetic workload task'))


# now we've scanned the logs, we can look at the two collections of tasks that
# we've created and create relevant subspans to relate them.

task_ids = set(list(synth_id_to_span_uuid_submit.keys())+ list(synth_id_to_span_uuid_remote.keys()))

for task_id in task_ids:
    print(f"checking for subspan relationship for task {task_id}")
    if task_id in synth_id_to_span_uuid_submit and task_id in synth_id_to_span_uuid_remote:
        print(f"creating subspan relationship")
        submit_uuid = synth_id_to_span_uuid_submit[task_id]
        remote_uuid = synth_id_to_span_uuid_remote[task_id]
        cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (submit_uuid, remote_uuid, "remote"))


db.commit()
