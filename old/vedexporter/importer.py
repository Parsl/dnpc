#!/usr/bin/env python3
import re
import sqlite3
import uuid

# an importer for funcx synthetic workflow

def importfile(json_filename):

  db_name = "funcx.sqlite3"
  db = sqlite3.connect(db_name,
                       detect_types=sqlite3.PARSE_DECLTYPES |
                       sqlite3.PARSE_COLNAMES)

  cursor = db.cursor()

  cursor = db.cursor()
  cursor.execute("CREATE TABLE IF NOT EXISTS awslog ("
                 "entry TEXT PRIMARY KEY"
                 ")")

  # now we can import the aws cloudwatch logs

  import datetime
  import json

  with open(json_filename, "r") as f:
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
    # if "task_id" not in message_log_json_decoded:
    #  print(f"skipping message without task_id: {message_log_json_decoded}")
    #  continue

    # print(f"t: {type(message_json_decoded['log'])}")
    cursor.execute("INSERT INTO awslog (entry) VALUES (?) ON CONFLICT DO NOTHING", (message_json_decoded['log'], ))

  db.commit()

import os
l = os.listdir(".")
for fn in l:
  if fn.endswith(".json"):
    print(f"importing {fn}")
    importfile(fn)
  else:
    print(f"skipping {fn}")
