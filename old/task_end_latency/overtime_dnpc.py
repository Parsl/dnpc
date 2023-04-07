import dnpcsql.twoevents as twoevents

query = """
    SELECT start_event.time, end_event.time - start_event.time
    FROM event as start_event,
         event as end_event,
         span
    WHERE start_event.span_uuid = span.uuid
      AND end_event.span_uuid = span.uuid
      AND span.type = "parsl.try"
      AND start_event.type = "running_ended"
      AND end_event.type = "exec_done" ;
"""

twoevents.plot(query, "overtime_dnpc.png")
