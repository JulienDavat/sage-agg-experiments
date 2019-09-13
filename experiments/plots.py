import matplotlib.pyplot as plt
import numpy as np
import csv
import argparse
import glob
import os
import functools

parser = argparse.ArgumentParser(description='Plot an experiment using virtuoso bash file ')
parser.add_argument('path', metavar='path', type=str, help='Path to the experiment')
args = parser.parse_args()

path = args.path + '/*spy*'

# GET ALL FILES AND SORT THEM BY NAME
files = glob.glob(path)
queries = {}
for file in files:
    filename = os.path.basename(file)
    queryname = filename.split("-")
    queries[queryname[0]] = file
sortedQueries = sorted(queries.keys())

# READ ALL FILES
# name -> [status_code, http, time, bytes]
results = {}

x = []
errored = []
times = []
bytes = []

i = 0
for q in sortedQueries:
    print("Reading: " + queries[q])
    with open(queries[q],'r') as f:
        plots = csv.reader(f, delimiter=' ')
        for row in plots:
            results[q] = row
            # print(row)
            x.append(q)
            times.append(int(row[2]))
            if row[3] == '':
                errored.append(i)
                row[3] = 0
            i = i + 1
            bytes.append(int(row[3]))


fig, axes = plt.subplots(figsize=(14, 12), nrows=2, ncols=1)

bar0 = axes[0].bar(x, times, label=x)
bar1 = axes[1].bar(x, bytes, label=x)

for i in errored:
    bar0[i].set_color('r')
    bar1[i].set_color('r')

axes[0].set_ylabel("Execution time (ms)")
axes[1].set_ylabel("Answer's bytes length")
plt.xlabel("Queries")

axes[0].ticklabel_format(axis='y', useOffset=False, style='plain')
axes[0].set_yscale('log')
axes[1].ticklabel_format(axis='y', useOffset=False, style='plain')
axes[1].set_yscale('log')

print("Execution times:" + str(times))

gb = functools.reduce(lambda a,b : a+b, times)
print("Global execution time: " + str(gb) + " (ms) = " + str(gb/1000/60/60) + " Hours")
print("Answer's bytes length" + str(bytes))

# plt.show()
plt.savefig(fname=args.path + '/plot.png', quality=100, format='png', dpi=100)