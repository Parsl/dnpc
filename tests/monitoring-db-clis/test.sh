#!/bin/bash -ex

rm -f dnpc.sqlite3 *.out

# Check importer runs, but do not check stdout
# The sqlite3 output will be checked indirectly by
# subsequent "identical output" CLI tool runs.
python3 -m dnpcsql.import_parsl_runinfo ./runinfo

# check these command line tools produce expected output
python3 -m dnpcsql.list_span_types > list_span_types.out
diff list_span_types.out list_span_types.out.expected

python3 -m dnpcsql.list_event_sequences > list_event_sequences.out
diff list_event_sequences.out list_event_sequences.out.expected

# check these tools run, but do not check output makes sense
python3 -m dnpcsql.dag_span_types
[ -f spandag.png ]

python3 -m dnpcsql.plot_task_durations
python3 -m dnpcsql.plot_spans_in_each_state

echo Test completed successfully
