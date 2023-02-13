# lists span types in the database

import sqlite3

if __name__ == "__main__":
    db_name = "dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    query = "SELECT DISTINCT type FROM span;"

    cursor = db.cursor()

    rows = list(cursor.execute(query))

    for r in rows:
      print(r[0])
 
