import matplotlib.pyplot as plt
import numpy as np
import csv
import glob
import os
import functools

def plotSage(path):
    # GET ALL FILES AND SORT THEM BY NAME
    files = glob.glob(path + '/*spy*')
    queries = {}
    for file in files:
        filename = os.path.basename(file)
        queryname = filename.split("-")
        queries[queryname[0]] = file
    sortedQueries = sorted(queries.keys())

    # READ ALL FILES
    # name -> [status_code, http, time, bytes]
    results = {}
    data = []
    x = []
    times = []
    bytes = []
    https = []

    for q in sortedQueries:
        # print("Reading: " + queries[q])
        with open(queries[q],'r') as f:
            plots = csv.reader(f, delimiter=',')
            for row in plots:
                data.append(row)
                results[q] = row
                # print(row)
                x.append(q)
                times.append(float(row[0]))
                bytes.append(float(row[8]))
                https.append(float(row[1]))

                #print(row)
                #print(float(row[0]), float(row[5]), float(row[1]))

    fig, axes = plt.subplots(figsize=(14, 12), nrows=2, ncols=1)

    bar0 = axes[0].bar(x, times, label=x)
    bar1 = axes[1].bar(x, bytes, label=x)

    axes[0].set_ylabel("Execution time (ms)")
    axes[1].set_ylabel("Answer's bytes length")
    plt.xlabel("Queries")

    axes[0].ticklabel_format(axis='y', useOffset=False, style='plain')
    axes[0].set_yscale('log')
    axes[1].ticklabel_format(axis='y', useOffset=False, style='plain')
    axes[1].set_yscale('log')

    # print("Execution times:" + str(times))

    gb = functools.reduce(lambda a, b: a+b, times)
    print("Global execution time: " + str(gb) + " (seconds) = " + str(gb/60/60) + " Hours")
    # print("Answer's bytes length" + str(bytes))

    csv_file = open(path + "/sage.csv", "w+")
    for dat in data:
        csv_file.write(','.join(dat) + '\n')
    csv_file.close()

    # plt.show()
    plt.savefig(fname=path + '/plot.png', quality=100, format='png', dpi=100)
    plt.close(fig)
    return x

def multiPlotSageNormalAgg(normal = [], agg = [], labels = []):
    times_normal = []
    bytes_normal = []
    http_normal = []
    times_agg = []
    bytes_agg = []
    http_agg = []


    for row in normal:
        times_normal.append(float(row[0]))
        bytes_normal.append(float(row[8]))
        http_normal.append(float(row[1]))
    for row in agg:
        times_agg.append(float(row[0]))
        bytes_agg.append(float(row[8]))
        http_agg.append(float(row[1]))

    figMulti, axesMulti = plt.subplots(figsize=(14, 12), nrows=3, ncols=1, sharex=True)

    index = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    # time
    bar01 = axesMulti[0].bar(index - width/2, times_normal, width, label='Normal')
    bar02 = axesMulti[0].bar(index + width/2, times_agg, width, label='Optimized')

    # bytes
    bar11 = axesMulti[1].bar(index - width/2, bytes_normal, width, label='Normal')
    bar12 = axesMulti[1].bar(index + width/2, bytes_agg, width, label='Optimized')

    #http
    bar21 = axesMulti[2].bar(index - width/2, http_normal, width, label='Normal')
    bar22 = axesMulti[2].bar(index + width/2, http_agg, width, label='Optimized')

    axesMulti[0].set_ylabel("Execution time (s)")
    axesMulti[1].set_ylabel("Total transfer size (bytes)")
    axesMulti[2].set_ylabel("Total HTTP requests")

    plt.xlabel("Queries")

    axesMulti[0].set_xticks(index)
    axesMulti[0].set_xticklabels(labels)
    axesMulti[1].set_xticks(index)
    axesMulti[1].set_xticklabels(labels)
    axesMulti[2].set_xticks(index)
    axesMulti[2].set_xticklabels(labels)

    axesMulti[0].ticklabel_format(axis='y', useOffset=False, style='plain')
    #axesMulti[0].set_yscale('log')
    axesMulti[1].ticklabel_format(axis='y', useOffset=False, style='plain')
    #axesMulti[1].set_yscale('log')
    axesMulti[2].ticklabel_format(axis='y', useOffset=False, style='plain')
    #axesMulti[1].set_yscale('log')

    axesMulti[0].legend()
    axesMulti[1].legend()
    axesMulti[2].legend()
    figMulti.tight_layout()

    plt.savefig(fname='multi.png', quality=100, format='png', dpi=100)
    plt.close(figMulti)