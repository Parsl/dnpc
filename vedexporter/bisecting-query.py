#!/usr/bin/env python3
import datetime
import json
import os
import re
import subprocess
import time

one_day = 60 * 60 * 24

def get_logs(start_t, end_t):
  print(f"Scanning from  {datetime.datetime.utcfromtimestamp(start_t).strftime('%Y-%m-%dT%H:%M:%SZ')} for {end_t - start_t} seconds")

  if end_t <= start_t:
    print("End time is earlier than start time - nothing to query")


  if end_t - start_t > one_day:
    print("More than one day: dividing into one day chunks without querying AWS")
    r_start = start_t
    r_end = r_start + one_day

    if r_end > end_t:
      get_logs(r_start, end_t)
    else:
      get_logs(r_start, r_end)
      get_logs(r_end, end_t)

    return

  #aws --profile funcx logs start-query --log-group-name /aws/containerinsights/funcx-prod/application --start-time $before --end-time $now --query-string 'fields @timestamp, @message | sort @timestamp desc | limit 10000 | filter log_processed.user_id = 34'
  cmd = ["aws",
         "--profile", "funcx",
         "logs", "start-query",
         "--log-group-name" ,"/aws/containerinsights/funcx-prod/application",
         "--start-time", f"{int(start_t)}",
         "--end-time", f"{int(end_t)}",
         "--query-string", "fields @timestamp, @message | sort @timestamp desc | limit 10000 | filter (log_processed.user_id = 34 or log_processed.user_id = 1 or log_processed.user_id = 252)"
        ]

  cp = subprocess.run(cmd, capture_output=True)
  print("start-query complete")
  #  print(f"complete process: {cp}")
  #   print(f"output = {cp.stdout}")
  assert cp.returncode == 0
  r = json.loads(cp.stdout)
  print(f"queryid = {r['queryId']}")

  # now periodically poll for results, with exponential backoff
  poll_delay = 60
  status = "Running"
  while status == "Running":
    if status == "Running":
      print(f"not finished yet - waiting {poll_delay}s to poll again")
      time.sleep(poll_delay)
      poll_delay *= 2

    print("polling")

    #aws --profile funcx logs get-query-results --query-id=b95b790d-8daa-4801-b700-8e93d8959ab2 
    cmd = ["aws",
           "--profile", "funcx",
           "logs", "get-query-results",
           f"--query-id={r['queryId']}"
          ]
    cp2 = subprocess.run(cmd, capture_output=True)

    print("get-query-results complete")
    # print(f"CompleteProcess: {cp2}")

    r2 = json.loads(cp2.stdout)

    status = r2['status']
    print(f"new status: {status}")

  matched = r2['statistics']['recordsMatched']
  results = r2['results']
  print(f"Matched {matched} records, with {len(results)} returned")

  if len(results) == matched:
    with open(f"q-{start_t}-{end_t}-full.json", "w") as f:
      f.write(json.dumps(r2))
  else:
    print(f"Some match results were not returned - making {n_split} smaller queries")
    with open(f"q-{start_t}-{end_t}-partial.json", "w") as f:
      f.write(json.dumps(r2))
    n_split = (matched / 10000) * 3 # doesn't need to be int. the bigger the factor, the more queries, focused over a smaller time we will do. bigger probably = better for uneven workloads where all the interesting stuff is likely to be in one segment even as we zoom in.
    t_split = (end_t - start_t) / n_split
    e = start_t
    while e <= end_t:
      s = e
      e += t_split
      get_logs(s,e)
    


  return


def get_latest():
  # this is a bodge to get access to the latest sqlite3 (at time of
  # writing) because unixepoch is not in the underlying distro
  # sqlite3 on server.
  os.system("~/importer/sqlite-tools-linux-x86-3400000/sqlite3 funcx.sqlite3  -cmd 'select unixepoch(substr(json_extract(entry, \"$.asctime\"),1,19)) as t from awslog order by t desc limit 1;' '.exit' -noheader > time.txt")

  with open("time.txt","r") as f:
      t = int(f.readline())
  print(f"got latest time from database: {t}")
  return t

if __name__ == "__main__":

  # default_lookback = 60 * 60 * 24 * 28 * 3 # around 3 months
  # default_lookback = 60 * 60 * 24 * 28 # around 1 month
  # default_lookback = 60 * 60 * 24 # 1 week
  default_lookback = 60 * 60 # 1 hour

  now = time.time()
  start = get_latest() - default_lookback
  results = get_logs(start, now)

if __name__ == "x__main__":
  print("infill mode")
  import os
  l = os.listdir(".")
  print(l)
  # q-1663137268.4907527-1663157520.4160225-full.json
  re1 = re.compile('^q-(.*)-(.*)-full.json$')
  t = []
  for d in l:
    m = re1.match(d)
    if m:
      print(f"{m[1]} -> {m[2]}")
      t.append( (float(m[1]), float(m[2])))
  t.sort()
  print(t)

  # for ix in range(len(t) - 1):
  for ix in range(len(t)-1):
    if t[ix + 1][0] <= t[ix][1]:
      print("Match")
    else:
      start_t = t[ix][1]
      end_t = t[ix+1][0]
      print(f"GAP: {start_t} to {end_t}")
      get_logs(start_t, end_t) 

  lasts = [y for (x,y) in t]

  lasts.sort()

  print(lasts)
