"""An importer for metrics written out inside arbitrary log files
This importer expects to find many lines, some of which contain
the keyword METRIC. For example:

1669383194.390360 2022-11-25 13:33:14 MainProcess-30062 FlowControl-Thread-140018264450816 parsl.dataflow.strategy:217 _general_strategy DEBUG: METRIC STRATEGY htex_Local active_tasks=3 running_blocks=2 pending_blocks=0 active_blocks=2 active_slots=2

The import will take unix timestamp from the first column, ignore everything
else before the keyword METRIC and intrepret the rest of the line as a
sequence of space delimited fields:
* span type
* span identifier (unique wrt this log file and span type)
* many of:
    metric=value
  fields, where metric is a C style identifier, and value is a number
  (deliberately, what kind of number is left ambiguous)
"""

import re

def process_logfile(filename: str) -> None:
    metric_re = re.compile("^([0-9\.]*) .* METRIC ([^ ]*) ([^ ]*) (.*)\n$")

    with open("/home/benc/parsl/src/parsl/runinfo/002/parsl.log") as logfile:
        for l in logfile:
            # print(f"** {l} <<")
            m = metric_re.match(l)
            if m:
                timestamp = float(m[1])
                span_type = m[2]
                span_id = m[3]
                metrics = m[4]
                print(f"At {timestamp}, {span_type}/{span_id} has metrics: {metrics}")
                metrics_split = metrics.split()
                print(metrics_split)
                for m in metrics_split:
                  kv = m.split('=')
                  assert len(kv) == 2
                  print(f"key {kv[0]} value {kv[1]}")

if __name__ == "__main__":
    print("log metric importer")

    process_logfile("/home/benc/parsl/src/parsl/runinfo/348/parsl.log")

