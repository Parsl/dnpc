import matplotlib.pyplot as plt
import numpy as np
import scipy.stats
import sqlalchemy
import sqlite3

def plot(query, output_filename, start_name, end_name):
    """Given an SQL query that returns two columns: a start time, and a
    duration, of something (for example the start time and duration of a
    task, plot various graphs about those periods.
    """
    xdata, ydata = get_data(query)
    plot_by_time(xdata, ydata, output_filename, start_name, end_name)
    plot_kde(xdata, ydata, "kde-"+output_filename, start_name, end_name)
    plot_histo(ydata, "histo-"+output_filename, start_name, end_name)

def get_data(query):
    db_name = "./dnpc.sqlite3"
    db = sqlite3.connect(db_name,
                     detect_types=sqlite3.PARSE_DECLTYPES |
                     sqlite3.PARSE_COLNAMES)

    cursor = db.cursor()

    rows = list(cursor.execute(query))

    print(f"there are {len(rows)} relevant status transitions in the db")

    xdata = np.array([float(x) for (x,y) in rows])

    # only normalise against minimum if the minimum actually exists:
    # if there is no data, there won't be a minimum, because the
    # arrays will be empty.
    if len(rows) > 0:
        xdata = xdata - xdata.min()

    ydata = np.array([float(y) for (x,y) in rows])

    return xdata, ydata


def plot_by_time(xdata, ydata, output_filename, start_name, end_name):

    fig, ax = plt.subplots()

    # ax.hist(durations_running_ended_exec_done, bins=100, color="#0000FF")
    ax.scatter(x=xdata, y=ydata, s=1)

    plt.xlabel(f"clock time of end event ({end_name})")
    plt.ylabel(f"s taken between events ({start_name} to {end_name}")

    plt.savefig(output_filename)

def plot_histo(ydata, output_filename, start_name, end_name):

    fig, ax = plt.subplots()

    ax.hist(ydata, bins=10, color="#0000FF")
    ax.axvline(ydata.mean(), linestyle='dashed', linewidth=1, label="mean", color = "#009900")
    ax.axvline(np.percentile(ydata, 50), linestyle='dashed', linewidth=1, label="median", color = "#FF00AA")
    ax.axvline(np.percentile(ydata, 95), linestyle='dashed', linewidth=1, label="95%", color = "#AA0000")
    ax.axvline(np.percentile(ydata, 100), linestyle='dashed', linewidth=1, label="maximum", color = "#FF4400")
    ax.legend()

    plt.xlabel(f"duration (s) between {start_name} and {end_name}")
    plt.ylabel("number of tasks in bin")

    plt.savefig(output_filename)

def plot_kde(xdata, ydata, output_filename, start_name, end_name):
    """
    Credit to:
    https://www.python-graph-gallery.com/85-density-plot-with-matplotlib
    """
    print("calc kde")
    k = scipy.stats.kde.gaussian_kde([xdata,ydata])
    nbins=300
    print("generate grid")

    # TODO: investigate the type error that is ignored here:
    # dnpcsql/twoevents.py:75: error: Slice index must be an integer or None  [misc]
    xi, yi = np.mgrid[xdata.min():xdata.max():nbins*1j, ydata.min():ydata.max():nbins*1j]  # type: ignore

    print("flatten")
    zi = k(np.vstack([xi.flatten(), yi.flatten()]))
    print("plot colourmesh")
    plt.pcolormesh(xi, yi, zi.reshape(xi.shape), shading='auto')
    print("save")
    plt.savefig(output_filename)
