import task_end_latency.twoevents as twoevents

query = """
    SELECT start_event.time, start_event.time - end_event.time
    FROM event as start_event,
         event as end_event,
         span as start_span,
         span as end_span,
         subspan
    WHERE start_event.span_uuid = start_span.uuid
      AND start_event.type = "exec_done"
      AND start_span.type = "parsl.try"
      AND start_span.uuid = subspan.superspan_uuid
      AND end_span.uuid = subspan.subspan_uuid
      AND end_span.type = "workqueue.task"
      AND end_event.type = "WAITING_RETRIEVAL"
      AND end_event.span_uuid = end_span.uuid
    ;
"""

twoevents.plot(query, "WAITING_RETRIEVAL_to_exec_done.png")
