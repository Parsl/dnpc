#!/bin/bash -ex

rm -f dnpc.sqlite3 *.out

python3 -m dnpcsql.import_parsl_runinfo ./runinfo

python3 -m dnpcsql.list_span_types > list_span_types.out

diff list_span_types.out list_span_types.out.expected


echo Test completed successfully
