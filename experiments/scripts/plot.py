import csv
import argparse
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description='Plot an experiment using virtuoso bash file ')
parser.add_argument('-dir', nargs='+', help='All dir to parse')
parser.add_argument('-o', action='store', default='average.csv', help='Output file')
args = parser.parse_args()

print("Output: ", args.o)

virtuoso = []
sage = []

for dir in args.dir:
    print('Processing:', dir)
    with open(dir + '/virtuoso-average.csv', 'r') as f:
        rows = csv.reader(f, delimiter=',')
        total_http_requests = 0
        total_traffic = 0
        total_execution_time = 0
        for row in rows:
            total_http_requests += float(row[1])
            total_execution_time += float(row[2])
            total_traffic += float(row[3])
        virtuoso.append([total_http_requests, total_execution_time, total_traffic])
    with open(dir + '/sage-average.csv', 'r') as f:
        rows = csv.reader(f, delimiter=',')
        total_http_requests = 0
        total_traffic = 0
        total_execution_time = 0
        for row in rows:
            total_http_requests += float(row[1])
            total_execution_time += float(row[0]) * 1000
            total_traffic += float(row[2])
        sage.append([total_http_requests, total_execution_time, total_traffic])

print("Virtuoso: ", virtuoso)
print("Sage: ", sage)

fig, axes = plt.subplots(figsize=(10, 3), nrows=1, ncols=3)

datasets = ["bsbm10", "bsbm100", "bsbsm1k"]

plt.subplot(131)
plt.plot(datasets, list(map(lambda e: e[0], virtuoso)), label="virtuoso")
plt.plot(datasets, list(map(lambda e: e[0], sage)), label="sage")
plt.title('Number of HTTP Requests')
plt.yscale('log')
plt.subplot(132)
plt.plot(datasets, list(map(lambda e: e[1], virtuoso)), label="virtuoso")
plt.plot(datasets, list(map(lambda e: e[1], sage)), label="sage")
plt.title('Execution time (ms)')
plt.yscale('log')
plt.subplot(133)
plt.plot(datasets, list(map(lambda e: e[2], virtuoso)), label="virtuoso")
plt.plot(datasets, list(map(lambda e: e[2], sage)), label="sage")
plt.title('Traffic')
plt.yscale('log')
#plt.show()
plt.legend()
plt.savefig(fname=args.o, quality=100, format='png', dpi=100)