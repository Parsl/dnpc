import re
import sqlite3
import uuid

# an importer for funcx synthetic workflow


log_filename = "funcxsynth2/synthetic.log"

db_name = "dnpc.sqlite3"
db = sqlite3.connect(db_name,
                     detect_types=sqlite3.PARSE_DECLTYPES |
                     sqlite3.PARSE_COLNAMES)

cursor = db.cursor()

synth_id_to_span_uuid_remote = {}
synth_id_to_span_uuid_submit = {}

funcx_task_id_to_synth_id = {}

with open(log_filename, "r") as log:

    re_remote = re.compile("^[0-9.]+ .* FUNCREMOTE ([0-9]+) ([^ ]+) ([0-9.]+).*$")

    re_submit = re.compile("^([0-9.]+) .* FUNC ([0-9]+) ([^ \n]+).*$")
    re_bind = re.compile("^[0-9.]+ .* FUNCBIND ([0-9]+) ([^ \n]+).*$")

    #1659518799.373132 2022-08-03 11:26:39,373 MainProcess-221120 MainThread-140308941870912 synthetic_client:82 <module> INFO: FUNCBIND 3000 fb0d5bf0-124a-45c5-a1e2-b1b1bbe24d24


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

        m = re_bind.match(line)
        if m:
            synth_task_id = m[1]
            funcx_task_id = m[2]
            funcx_task_id_to_synth_id[funcx_task_id] = synth_task_id
           

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

# now we can import the aws cloudwatch logs

import datetime
import json

with open("funcxsynth2/aws.json", "r") as f:
  j = json.load(f)

assert j['status'] == "Complete"

# the interesting stuff for now in each log line is a tuple:
#    task_id, timestamp, status name

tuples = []

def get_value_field(fields, field_name):
  r = [f['value'] for f in fields if f['field'] == field_name]
  assert len(r) == 1
  return r[0]

for fields in j['results']:
  timestamp_field = get_value_field(fields, '@timestamp')
  message_field = get_value_field(fields, '@message')
  message_json_decoded = json.loads(message_field)
  message_log_json_decoded = json.loads(message_json_decoded['log'])
  if "task_id" not in message_log_json_decoded:
    print(f"skipping message without task_id: {message_log_json_decoded}")
    continue
  tuple = (datetime.datetime.fromisoformat(timestamp_field).timestamp(), message_log_json_decoded['task_id'], message_log_json_decoded['name'] + "--" + message_log_json_decoded['message'])
  tuples.append(tuple)

print("Extracted log line tupes:")
print(tuples)

# now regroup these tuples by task ID:

aws_tasks_by_id = {}

for (timestamp, tid, msg) in tuples:
  if tid not in aws_tasks_by_id:
    aws_tasks_by_id[tid] = []
  aws_tasks_by_id[tid].append( (timestamp, msg) )

print(aws_tasks_by_id)

# now each of these aws_task_by_id entries becomes a span,
# and a subspan of the relevant parent task, with each
# log line becoming an event.

funcx_task_id_to_cloudwatch_span_id = {}

for (funcx_task_id, events) in aws_tasks_by_id.items():
    print(f"Need to create a new span for cloudwatch task view for task {funcx_task_id}")
    span_uuid = str(uuid.uuid4())
    funcx_task_id_to_cloudwatch_span_id[funcx_task_id] = span_uuid
    cursor.execute("INSERT INTO span (uuid, type, note) VALUES (?, ?, ?)", (span_uuid, 'cloudwatch.task', 'Cloudwatch view of a task'))

    for (timestamp, msg) in events:
      print(f"Need to add an event for {msg}")
      event_uuid = str(uuid.uuid4())
      cursor.execute("INSERT INTO event (uuid, span_uuid, time, type, note) VALUES (?, ?, ?, ?, ?)", (event_uuid, span_uuid, timestamp, msg, 'progress event from cloudwatch'))

    # now bind this funcx task to a synth task
    print(funcx_task_id_to_synth_id[funcx_task_id])
    submit_uuid = synth_id_to_span_uuid_submit[funcx_task_id_to_synth_id[funcx_task_id]]
    print(f"creating subspan relationship")
    cursor.execute("INSERT INTO subspan (superspan_uuid, subspan_uuid, key) VALUES (?, ?, ?)", (submit_uuid, span_uuid, "cloudwatch"))

db.commit()
