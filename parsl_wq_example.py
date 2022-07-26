import sqlite3
import dnpcsql.parsl
import dnpcsql.workqueue

def main() -> None:
    print("dnpcsql parsl importer")

    connection = init_sql()

    dnpcsql.parsl.import_all(connection)

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
