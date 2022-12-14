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

import pandas as pd
import re

from typing import Dict

# one dataframe for each span type (aka metric type?)
# with dataframes added as they are discovered in the log
metric_dataframes: Dict[str, pd.DataFrame] = {}

def process_logfile(filename: str) -> None:
    metric_re = re.compile("^([0-9\.]*) .* METRIC ([^ ]*) ([^ ]*) (.*)\n$")

    with open(filename) as logfile:
        for l in logfile:
            # print(f"** {l} <<")
            m = metric_re.match(l)
            if m:
                timestamp = float(m[1])
                span_type = m[2]
                span_id = m[3]
                metrics = m[4]
                assert isinstance(metrics, str)
                print(f"At {timestamp}, {span_type}/{span_id} has metrics: {metrics}")
                metrics_split = metrics.split()
                print(metrics_split)

                if span_type in metric_dataframes:
                    dataframe = metric_dataframes[span_type]
                else:
                    dataframe = pd.DataFrame()

                ks = ['span_id', 'timestamp']
                vs = [span_id, timestamp]
                for m2 in metrics_split:
                    assert isinstance(m, str)
                    kv = m2.split('=')
                    assert len(kv) == 2
                    k = kv[0]
                    v = kv[1]
                    print(f"key {k} value {v}")
                    ks.append(k)
                    vs.append(v)
                print(ks)
                print(vs)
                new_df = pd.DataFrame([vs], columns=ks)
                print(new_df)
                dataframe = pd.concat([dataframe, new_df])
                metric_dataframes[span_type] = dataframe

if __name__ == "__main__":
    print("log metric importer")

    process_logfile("/home/benc/parsl/src/parsl/runinfo/353/parsl.log")

    print("metrics_dataframe dictionary:")
    print(metric_dataframes)
