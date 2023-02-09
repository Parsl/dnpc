import sqlite3

def main() -> None:
    print("dnpcsql schema creator")

    connection = init_sql()

    connection.commit()
    connection.close()


def init_sql() -> sqlite3.Connection:
    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    create_tables(db)

    return db


def create_tables(db: sqlite3.Connection) -> None:

    # All spans have a UUID that identifies them without
    # a containing superspan to name them.

    cursor = db.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS span ("
                   "uuid TEXT PRIMARY KEY,"
                   "type TEXT NOT NULL,"  # domain style
                   "note TEXT"
                   ")")

    # each event exists within exactly one span
    # it is identified by a UUID
    cursor.execute("CREATE TABLE IF NOT EXISTS event ("
                   "uuid TEXT PRIMARY KEY,"
                   "span_uuid TEXT REFERENCES span (uuid),"
                   "time TEXT NOT NULL,"
                   "type TEXT NOT NULL,"  # meaning comes from span.type
                   "note TEXT"
                   ")")

    # spans can be contained, DAG-style, within other spans.
    # the naming of subspans is a bit complicated - the name
    # should make sense within the parent span, and so a
    # subspan may have different names depending on which
    # superspan it is being viewed from.

    cursor.execute("CREATE TABLE IF NOT EXISTS subspan ("
                   "superspan_uuid TEXT REFERENCES span(uuid),"
                   "subspan_uuid TEXT REFERENCES span (uuid),"
                   "key TEXT NOT NULL,"
                   "note TEXT"
                   ")")

    cursor.execute("create index span_uuids on event (span_uuid)")
    cursor.execute("create index subspan_uuids_super on subspan (superspan_uuid)")
    cursor.execute("create index subspan_uuids_sub on subspan (subspan_uuid)")

if __name__ == "__main__":
     main()
