import matplotlib.pyplot as plt
import numpy as np
import scipy.stats
import sqlalchemy
import sqlite3

def plot(query, output_filename):
    xdata, ydata = get_data(query)
    plot_by_time(xdata, ydata, output_filename)
    plot_kde(xdata, ydata, "kde-"+output_filename)
    plot_histo(ydata, "histo-"+output_filename)

def get_data(query):
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

    return xdata, ydata


def plot_by_time(xdata, ydata, output_filename):

    fig, ax = plt.subplots()

    # ax.hist(durations_running_ended_exec_done, bins=100, color="#0000FF")
    ax.scatter(x=xdata, y=ydata, s=1)

    plt.xlabel("clock time of end event")
    plt.ylabel("s taken between events")

    plt.savefig(output_filename)

def plot_histo(ydata, output_filename):

    fig, ax = plt.subplots()

    ax.hist(ydata, bins=100, color="#0000FF")
    ax.axvline(ydata.mean(), linestyle='dashed', linewidth=1, label="mean", color = "#009900")
    ax.axvline(np.percentile(ydata, 50), linestyle='dashed', linewidth=1, label="median", color = "#FF00AA")
    ax.axvline(np.percentile(ydata, 95), linestyle='dashed', linewidth=1, label="95%", color = "#AA0000")
    ax.axvline(np.percentile(ydata, 100), linestyle='dashed', linewidth=1, label="maximum", color = "#FF4400")
    ax.legend()

    plt.xlabel("seconds between start,end")
    plt.ylabel("number of tasks in bin")

    plt.savefig(output_filename)

def plot_kde(xdata, ydata, output_filename):
    """
    Credit to:
    https://www.python-graph-gallery.com/85-density-plot-with-matplotlib
    """
    print("calc kde")
    k = scipy.stats.kde.gaussian_kde([xdata,ydata])
    nbins=300
    print("generate grid")
    xi, yi = np.mgrid[xdata.min():xdata.max():nbins*1j, ydata.min():ydata.max():nbins*1j]
    print("flatten")
    zi = k(np.vstack([xi.flatten(), yi.flatten()]))
    print("plot colourmesh")
    plt.pcolormesh(xi, yi, zi.reshape(xi.shape), shading='auto')
    print("save")
    plt.savefig(output_filename)
