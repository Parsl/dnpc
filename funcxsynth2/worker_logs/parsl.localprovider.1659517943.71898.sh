
export JOBNAME=$parsl.localprovider.1659517943.71898
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
funcx-manager --debug  -c 1.0 --poll 10 --task_url=tcp://127.0.0.1:54899 --result_url=tcp://127.0.0.1:54517 --logdir=/root/.funcx/default/HighThroughputExecutor/worker_logs --block_id=2 --hb_period=30 --hb_threshold=120 --worker_mode=no_container --container_cmd_options='' --scheduler_mode=hard --worker_type=RAW --available-accelerators 
}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "1" == "1" ]] && echo "Launching worker: $COUNT"
    CMD $COUNT &
    PIDS="$PIDS $!"
done

ALLFAILED=1
ANYFAILED=0
for PID in $PIDS ; do
    wait $PID
    if [ "$?" != "0" ]; then
        ANYFAILED=1
    else
        ALLFAILED=0
    fi
done

[[ "1" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
