import dnpcsql.twoevents

parsl_task_invoked_to_returned = """
     SELECT event_start.time,
            event_end.time - event_start.time
       FROM event as event_start,
            event as event_end
      WHERE event_start.span_uuid = event_end.span_uuid
        and event_start.type = "invoked"
        and event_end.type="returned";
"""

dnpcsql.twoevents.plot(parsl_task_invoked_to_returned, "parsl-task-durations", "parsl app invoked", "parsl app returned")
