export now=$(date +%s)
export before=$(( $now - 86400 ))

echo before = $(date --date=@$before)

aws --profile funcx logs start-query --log-group-name /aws/containerinsights/funcx-dev/application --start-time $before --end-time $now --query-string 'fields @timestamp, @message | sort @timestamp desc | limit 10000 | filter log_processed.task_group_id = "99340f18-7a58-4f85-bea5-915ba16a3ea9"'

