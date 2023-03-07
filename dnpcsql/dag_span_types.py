# draw a graph of which span types are directly related to span types,
# by either subspan containment or by facet equivalence.

# The subspan components will usually be a DAG (but aren't constrained to
# be so - an instance of span type A might contain an instance of span
# type B, and another instance of span type B might contain an instance
# of span type A. (a usecase of this might be some kind of recursive
# behaviour such as putting join-dependency execution as part of a join
# app in parsl)

# facet-equivalence is not directed - even though the table has a left
# and a right uuid, the meaning is the same either way round.

import os
import sqlite3

if __name__ == "__main__":

    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)
    cursor = db.cursor()

    get_spans_query = "SELECT DISTINCT type FROM span;"
    span_type_rows = list(cursor.execute(get_spans_query))

    get_subspans_query = "select distinct a.type, b.type from subspan, span as a, span as b where subspan.superspan_uuid = a.uuid and subspan.subspan_uuid = b.uuid"
    subspan_relation_rows = list(cursor.execute(get_subspans_query))

    get_facet_query = "select distinct a.type, b.type from facet, span as a, span as b where facet.left_uuid = a.uuid and facet.right_uuid = b.uuid"
    facet_relation_rows = list(cursor.execute(get_facet_query))

    with open("tmp.dot","w") as f:
        print("digraph G {", file=f)

        for r in span_type_rows:
            print(f"\"{r[0]}\"; ", file=f);

        for r in subspan_relation_rows:
            print(f"\"{r[0]}\" -> \"{r[1]}\"; ", file=f);

        for r in facet_relation_rows:
            print(f"\"{r[0]}\" -> \"{r[1]}\" [ color = \"blue\", dir = both  ]; ", file=f);

        print("}", file=f)

    os.system("dot -Tpng -o spandag.png tmp.dot")

