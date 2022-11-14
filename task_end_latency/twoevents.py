import matplotlib.pyplot as plt
import numpy as np
import sqlalchemy
import sqlite3

def plot(query, output_filename):
    db_name = "./dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                     detect_types=sqlite3.PARSE_DECLTYPES |
                     sqlite3.PARSE_COLNAMES)

    cursor = db.cursor()

    rows = list(cursor.execute(query))
    assert len(rows) == 32000

    print(f"there are {len(rows)} relevant status transitions in the db")

    xdata = np.array([float(x) for (x,y) in rows])
    xdata = xdata - xdata.min()

    ydata = np.array([float(y) for (x,y) in rows])

    fig, ax = plt.subplots()

    # ax.hist(durations_running_ended_exec_done, bins=100, color="#0000FF")
    ax.scatter(x=xdata, y=ydata, s=1)

    plt.xlabel("clock time of end event")
    plt.ylabel("s taken between events")

    plt.savefig(output_filename)

