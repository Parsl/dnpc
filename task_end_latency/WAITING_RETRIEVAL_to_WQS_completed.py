import task_end_latency.twoevents as twoevents

query = """
    SELECT start_event.time, start_event.time - end_event.time
    FROM event as start_event,
         event as end_event,
         span as span
    WHERE start_event.span_uuid = span.uuid
      AND start_event.type = "WQS_completed"
      AND span.type = "workqueue.task"
      AND end_event.type = "WAITING_RETRIEVAL"
      AND end_event.span_uuid = span.uuid
    ;
"""

twoevents.plot(query, "WAITING_RETRIEVAL_to_WQS_completed.png")
