import os
import sqlite3
import sys
import dnpcsql.parsl
import dnpcsql.workqueue
import dnpcsql.twoevents as twoev
from dnpcsql.schema import create_tables

def main() -> None:
    print("dnpcsql parsl runinfo importer")

    runinfo=sys.argv[1]
    print(f"Will import from runinfo: {runinfo}")

    if os.path.exists("dnpc.sqlite3"):
        print("Removing previous dnpcsql database")
        os.remove("dnpc.sqlite3")


    connection = init_sql()

    create_tables(connection)

    dnpcsql.parsl.import_rundir_root(connection, runinfo=runinfo)

    connection.commit()
    connection.close()

    # now plot parsl-level task durations

    parsl_task_invoked_to_returned = """
     SELECT event_start.time,
            event_end.time - event_start.time
       FROM event as event_start,
            event as event_end
      WHERE event_start.span_uuid = event_end.span_uuid
        and event_start.type = "invoked"
        and event_end.type="returned";
    """

    twoev.plot(parsl_task_invoked_to_returned, "parsl-task-durations", "parsl app invoked", "parsl app returned")


def init_sql() -> sqlite3.Connection:
    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    return db

if __name__ == "__main__":
    main()
