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

    dnpcsql.parsl.import_rundir_root(db=connection, runinfo=runinfo)

    connection.commit()
    connection.close()


def init_sql() -> sqlite3.Connection:
    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    return db

if __name__ == "__main__":
    main()
