#!/bin/bash -ex

rm -f dnpc.sqlite3

# there's no CLI for this - the raw WQ importer is exposed only through the
# Python API
python3 ./import_wq.py

# check these command line tools produce expected output
python3 -m dnpcsql.list_span_types > list_span_types.out
diff list_span_types.out list_span_types.out.expected

python3 -m dnpcsql.list_event_sequences workqueue.task > list_event_sequences.out
diff list_event_sequences.out list_event_sequences.out.expected

echo Test completed successfully
