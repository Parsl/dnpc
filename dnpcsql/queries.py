
def events_for_root_span_type(root_span_type: str):
    return f"""
with recursive

  descs(root_span_uuid, span_uuid) as (
    select span.uuid, span.uuid from span where span.type="{root_span_type}"
    union
    select descs.root_span_uuid, subspan.subspan_uuid
      from subspan, descs
     where subspan.superspan_uuid = descs.span_uuid
    union
    select descs.root_span_uuid, facet.right_uuid
     from facet, descs
     where facet.left_uuid = descs.span_uuid
    union
    select descs.root_span_uuid, facet.left_uuid
     from facet, descs
     where facet.right_uuid = descs.span_uuid
  )

  select descs.root_span_uuid, event.time, span.type, event.type, event.uuid
    from descs, span, event
   where span.uuid = descs.span_uuid
     and event.span_uuid = span.uuid
order by root_span_uuid, event.time;
    """

