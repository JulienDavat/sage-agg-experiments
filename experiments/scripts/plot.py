import csv
import argparse
import matplotlib.pyplot as plt
import numpy as np
import itertools

parser = argparse.ArgumentParser(description='Plot an experiment, please provide a folder where results are located eg: output/second')
parser.add_argument('-dir', action='store', help='root dir of results, eg: output/')
# parser.add_argument('-queries', action='store', help='queries location')
parser.add_argument('-o', action='store', help='Output file')
args = parser.parse_args()

if args.dir is None or args.o is None:
    parser.print_help()
    parser.exit(1)

print("Output: ", args.o)


datasets = ['bsbm10', 'bsbm100', 'bsbm1k']
buffer_size = [0, 100000, 1000000, 1000000000]
buffer_size_strings = ["0", "100Kb", "1Mb", "1Gb"]

tpf_dir = args.dir + "/tpf/"
sage_dir = args.dir + "/sage/"
virtuoso_dir = args.dir + "/virtuoso/"

virtuoso = []
sage = dict()
tpf = []
queries = dict()
distinct = [3,4,5,6,7,9,10,11,12,13,15,16,17,19,20,21]
non_distinct = [3,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44]
toProcess = list(range(1, 53))

print(toProcess)
for d in datasets:
    with open(virtuoso_dir + 'average-{}.csv'.format(d), 'r') as f:
        rows = csv.reader(f, delimiter=',')
        total_http_requests = 0
        total_traffic = 0
        total_execution_time = 0
        r = 1
        for row in rows:
            if r not in toProcess:
                pass
            else:
                total_http_requests += float(row[1])
                total_execution_time += float(row[2])
                total_traffic += float(row[3])
                if r not in queries:
                    queries[r] = dict()
                if "virtuoso" not in queries[r]:
                    queries[r]["virtuoso"] = []
                queries[r]["virtuoso"].append([float(row[1]), float(row[2]), float(row[3])])
            r += 1
        virtuoso.append([total_http_requests, total_execution_time, total_traffic])

    with open(sage_dir + 'average-{}-normal.csv'.format(d), 'r') as f:
        rows = csv.reader(f, delimiter=',')
        total_http_requests = 0
        total_traffic = 0
        total_execution_time = 0
        r = 1
        for row in rows:
            if r not in toProcess:
                pass
            else:
                total_http_requests += float(row[1])
                total_execution_time += float(row[0]) * 1000
                t = float(row[2]) - 36 * (float(row[1]) - 1)
                total_traffic += t
                print(t)
                if r not in queries:
                    queries[r] = dict()
                if "sage" not in queries[r]:
                    queries[r]["sage"] = dict()
                if "normal" not in queries[r]["sage"]:
                    queries[r]["sage"]["normal"] = []
                queries[r]["sage"]["normal"].append([float(row[1]), float(row[0]) * 1000, t])
            r += 1
        if "normal" not in sage:
            sage["normal"] = []
        sage["normal"].append([total_http_requests, total_execution_time, total_traffic])

    for b in buffer_size:
        with open(sage_dir + 'average-{}-b-{}.csv'.format(d, b), 'r') as f:
            rows = csv.reader(f, delimiter=',')
            total_http_requests = 0
            total_traffic = 0
            total_execution_time = 0
            r = 1
            for row in rows:
                if r not in toProcess:
                    pass
                else:
                    total_http_requests += float(row[1])
                    total_execution_time += float(row[0]) * 1000
                    t = float(row[2]) - 36 * (float(row[1]) - 1)
                    total_traffic += t
                    print(t)
                    if r not in queries:
                        queries[r] = dict()
                    if "sage" not in queries[r]:
                        queries[r]["sage"] = dict()
                    if b not in queries[r]["sage"]:
                        queries[r]["sage"][b] = []
                    queries[r]["sage"][b].append([float(row[1]), float(row[0]) * 1000, t])
                r += 1
            if b not in sage:
                sage[b] = []
            sage[b].append([total_http_requests, total_execution_time, total_traffic])

    with open(tpf_dir + 'average-{}.csv'.format(d), 'r') as f:
        rows = csv.reader(f, delimiter=',')
        total_http_requests = 0
        total_traffic = 0
        total_execution_time = 0
        r = 1
        for row in rows:
            if r not in toProcess:
                pass
            else:
                total_http_requests += float(row[2])
                total_execution_time += float(row[1]) * 1000
                total_traffic += float(row[3])
                if r not in queries:
                    queries[r] = dict()
                if "tpf" not in queries[r]:
                    queries[r]["tpf"] = []
                queries[r]["tpf"].append([float(row[2]), float(row[1]) * 1000, float(row[3])])
            r += 1

        tpf.append([total_http_requests, total_execution_time, total_traffic])


print("Virtuoso: ", virtuoso)
print("Sage: ", sage[0])
print("Tpf: ", tpf)


def xticks(barWidth):
    plt.xticks([r + barWidth for r in range(len(sage[0]))], datasets)

def scale(log=True):
    if log:
        plt.yscale('log')

def main(log=True):
    fig, axes = plt.subplots(figsize=(10, 3), nrows=1, ncols=3)

    barWidth = 0.25

    r1 = np.arange(len(datasets))
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]
    plt.subplot(131)
    plt.bar(r1, list(map(lambda e: e[0], virtuoso)), label="virtuoso", width=barWidth)
    plt.bar(r2, list(map(lambda e: e[0], sage[0])), label="sage", width=barWidth)
    plt.bar(r3, list(map(lambda e: e[0], tpf)), label="tpf", width=barWidth)
    plt.title('Number of HTTP Requests')
    scale(log)
    xticks(barWidth)

    plt.subplot(132)
    plt.bar(r1, list(map(lambda e: e[1], virtuoso)), label="virtuoso", width=barWidth)
    plt.bar(r2, list(map(lambda e: e[1], sage[0])), label="sage", width=barWidth)
    plt.bar(r3, list(map(lambda e: e[1], tpf)), label="tpf", width=barWidth)
    plt.title('Execution time (s)')
    scale(log)
    xticks(barWidth)

    plt.subplot(133)
    plt.bar(r1, list(map(lambda e: e[2], virtuoso)), label="virtuoso", width=barWidth)
    plt.bar(r2, list(map(lambda e: e[2], sage[0])), label="sage", width=barWidth)
    plt.bar(r3, list(map(lambda e: e[2], tpf)), label="tpf", width=barWidth)
    plt.title('Traffic')
    scale(log)
    xticks(barWidth)

    plt.legend()
    if log:
        plt.savefig(fname=args.o + '-log.png', quality=100, format='png', dpi=100)
    else:
        plt.savefig(fname=args.o, quality=100, format='png', dpi=100)
    plt.close()

# main()
# main(log=False)


def datasets_traffic(log=True):
    i = 0
    for d in datasets:

        # get all data
        data = []
        for b in buffer_size:
            print(b)
            data.append(sage[b][i])
        fig, axes = plt.subplots(figsize=(20, 6), nrows=1, ncols=3)
        x = np.arange(len(buffer_size))
        plt.subplot(131)
        plt.bar(x, list(map(lambda e: e[0], data)))
        scale(log)
        plt.title('Number of HTTP Requests')
        plt.xticks(x, buffer_size)

        plt.subplot(132)
        plt.bar(x, list(map(lambda e: e[1], data)))
        scale(log)
        plt.title('Execution time (s)')
        plt.xticks(x, buffer_size)

        plt.subplot(133)
        plt.bar(x, list(map(lambda e: e[2], data)))
        scale(log)
        plt.title('Traffic')
        plt.xticks(x, buffer_size)

        if log:
            plt.savefig(fname=args.o + '-sage-{}-log.png'.format(d), quality=100, format='png', dpi=100)
        else:
            plt.savefig(fname=args.o + '-sage-{}.png'.format(d), quality=100, format='png', dpi=100)
        plt.close()

        i += 1

# datasets_traffic()
# datasets_traffic(log=False)

# for b in buffer_size:
#     fig, axes = plt.subplots(figsize=(12, 12), nrows=3, ncols=1)
#     i = 0
#     for d in datasets:
#         plt.subplot(311 + i)
#         traffic = []
#         for q in queries:
#             traffic.append(queries[q]["sage"][b][i][2])
#         plt.bar(range(len(queries)), traffic)
#         plt.ylabel("traffic in bytes")
#         plt.xlabel("queries for {}".format(d))
#         #plt.yscale("log")
#         i += 1
#     plt.savefig(fname=args.o + '-all-query-sage-buffer-{}.png'.format(b), quality=100, format='png', dpi=100)
#     plt.close()

def one_query(query=21):
    fig, axes = plt.subplots(figsize=(16, 12), nrows=3, ncols=3, sharey=True, sharex=True)
    i = 0
    plt.title("Query {} ".format(query))
    j = 0
    ind = 331
    for d in datasets:

        http = []
        exec = []
        traffic = []
        for b in buffer_size:
            http.append(queries[query]["sage"][b][j][0])
            exec.append(queries[query]["sage"][b][j][1])
            traffic.append(queries[query]["sage"][b][j][2])

        plt.subplot(ind + i)
        plt.bar(range(len(buffer_size)), http)
        if i == 0:
            plt.title("http requests")
        if i == 0 or i == 3 or i == 6:
            plt.ylabel(d)
        plt.xticks(range(len(buffer_size)), buffer_size_strings)

        i += 1
        plt.subplot(ind + i)
        plt.bar(range(len(buffer_size)), exec)
        if i == 1:
            plt.title("execution time")
        plt.xticks(range(len(buffer_size)), buffer_size_strings)

        i += 1
        plt.subplot(ind + i)
        plt.bar(range(len(buffer_size)), traffic)
        if i == 2:
            plt.title("traffic", )
        plt.xticks(range(len(buffer_size)), buffer_size_strings)

        i += 1
        j += 1
    fig.suptitle("Number of Http requests, Execution time (s) and Traffic (bytes) "
                 "for query {} for each datasets".format(query))
    plt.savefig(fname=args.o + '-query-{}.png'.format(query), quality=100, format='png', dpi=100)
    plt.close()


def final(log=False):
    fig, axes = plt.subplots(figsize=(16, 12), nrows=3, ncols=1, sharex=True)
    options = {
        "linestyle": "dashed",
        "marker": 'o',
        "markersize": 8
    }
    plt.subplot(131)
    x = np.arange(len(datasets))
    plt.plot(x, list(map(lambda e: e[0], virtuoso)), label="virtuoso", **options)
    plt.plot(x, list(map(lambda e: e[0], sage["normal"])), label="sage", **options)
    plt.plot(x, list(map(lambda e: e[0], sage[0])), label="sage-agg-0", **options)
    plt.plot(x, list(map(lambda e: e[0], sage[100000])), label="sage-agg-100Kb", **options)
    plt.plot(x, list(map(lambda e: e[0], sage[1000000000])), label="sage-agg-1Gb", **options)
    plt.plot(x, list(map(lambda e: e[0], tpf)), label="tpf", **options)
    plt.xticks(x, datasets)
    plt.ylabel("Http requests")
    scale(log)
    plt.subplot(132)
    plt.plot(x, list(map(lambda e: e[1], virtuoso)), label="virtuoso", **options)
    plt.plot(x, list(map(lambda e: e[1], sage["normal"])), label="sage", **options)
    plt.plot(x, list(map(lambda e: e[1], sage[0])), label="sage-agg-0", **options)
    print(list(map(lambda e: e[2], sage[100000])), list(map(lambda e: e[2], sage[1000000000])))
    plt.plot(x, list(map(lambda e: e[1], sage[100000])), label="sage-agg-100Kb", **options)
    plt.plot(x, list(map(lambda e: e[1], sage[1000000000])), label="sage-agg-1Gb", **options)
    plt.plot(x, list(map(lambda e: e[1], tpf)), label="tpf", **options)
    plt.xticks(x, datasets)
    plt.ylabel("Execution time (s)")
    scale(log)
    plt.subplot(133)
    plt.plot(x, list(map(lambda e: e[2], virtuoso)), label="virtuoso", **options)
    plt.plot(x, list(map(lambda e: e[2], sage["normal"])), label="sage", **options)
    plt.plot(x, list(map(lambda e: e[2], sage[0])), label="sage-agg-0", **options)
    plt.plot(x, list(map(lambda e: e[2], sage[100000])), label="sage-agg-100Kb", **options)
    plt.plot(x, list(map(lambda e: e[2], sage[1000000000])), label="sage-agg-1Gb", **options)
    plt.plot(x, list(map(lambda e: e[2], tpf)), label="tpf", **options)
    plt.xticks(x, datasets)
    plt.ylabel("Traffic (bytes)")
    scale(log)
    plt.legend()
    fig.suptitle("Number of Http requests, Execution time (s) and Traffic (bytes) for each dataset")
    plt.savefig(fname=args.o + 'final.png', quality=100, format='png', dpi=100)
    plt.close()

final(log=True)