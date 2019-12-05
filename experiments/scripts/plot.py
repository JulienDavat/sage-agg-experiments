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

def process():
    datasets = ['bsbm10', 'bsbm100', 'bsbm1k']
    buffer_size = [0]
    buffer_size_strings = ["0"]

    tpf_dir = args.dir + "/tpf/"
    sage_dir = args.dir + "/sage/"
    virtuoso_dir = args.dir + "/virtuoso/"

    virtuoso = []
    sage = dict()
    tpf = []
    queries = dict()
    distinct = [3,4,5,6,7,9,10,11,12,13,15,16,17,19,20,21]
    non_distinct = [3,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44]
    toProcess = list(range(1, 32))

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
    return {
        "datasets": datasets,
        "buffer_size": buffer_size,
        "buffer_size_strings": buffer_size_strings,
        "sage": sage,
        "virtuoso": virtuoso,
        "tpf": tpf,
        "queries": queries
    }


def xticks(barWidth, sage, datasets):
    plt.xticks([r + barWidth for r in range(len(sage[0]))], datasets)

def scale(log=True):
    if log:
        plt.yscale('log')

def final(log=False, val={}):
    datasets = val["datasets"]
    virtuoso = val["virtuoso"]
    sage = val["sage"]
    tpf = val["tpf"]


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
    # plt.plot(x, list(map(lambda e: e[0], sage[100000])), label="sage-agg-100Kb", **options)
    # plt.plot(x, list(map(lambda e: e[0], sage[1000000000])), label="sage-agg-1Gb", **options)
    plt.plot(x, list(map(lambda e: e[0], tpf)), label="tpf", **options)
    plt.xticks(x, datasets)
    plt.ylabel("Http requests")
    scale(log)
    plt.subplot(132)
    plt.plot(x, list(map(lambda e: e[1], virtuoso)), label="virtuoso", **options)
    plt.plot(x, list(map(lambda e: e[1], sage["normal"])), label="sage", **options)
    plt.plot(x, list(map(lambda e: e[1], sage[0])), label="sage-agg-0", **options)
    # print(list(map(lambda e: e[2], sage[100000])), list(map(lambda e: e[2], sage[1000000000])))
    # plt.plot(x, list(map(lambda e: e[1], sage[100000])), label="sage-agg-100Kb", **options)
    # plt.plot(x, list(map(lambda e: e[1], sage[1000000000])), label="sage-agg-1Gb", **options)
    plt.plot(x, list(map(lambda e: e[1], tpf)), label="tpf", **options)
    plt.xticks(x, datasets)
    plt.ylabel("Execution time (s)")
    scale(log)
    plt.subplot(133)
    plt.plot(x, list(map(lambda e: e[2], virtuoso)), label="virtuoso", **options)
    plt.plot(x, list(map(lambda e: e[2], sage["normal"])), label="sage", **options)
    plt.plot(x, list(map(lambda e: e[2], sage[0])), label="sage-agg-0", **options)
    # plt.plot(x, list(map(lambda e: e[2], sage[100000])), label="sage-agg-100Kb", **options)
    # plt.plot(x, list(map(lambda e: e[2], sage[1000000000])), label="sage-agg-1Gb", **options)
    plt.plot(x, list(map(lambda e: e[2], tpf)), label="tpf", **options)
    plt.xticks(x, datasets)
    plt.ylabel("Traffic (bytes)")
    scale(log)
    plt.legend()
    fig.suptitle("Number of Http requests, Execution time (s) and Traffic (bytes) for each dataset")
    plt.savefig(fname=args.o + 'final.png', quality=100, format='png', dpi=100)
    plt.close()

final(log=True, val=process())


def process_quotas():
    quotas = ["150", "1500", "15000"]
    dataset = "bsbm1k"
    buffer_size = [0, 100000]
    buffer_size_strings = ["0", "100Kb"]

    sage_dir = args.dir + "/sage/"

    virtuoso = []
    sage = dict()
    tpf = []
    queries = dict()
    toProcess = list(range(1, 32))

    for quota in quotas:

        with open(args.dir + '/' + quota + '/average-{}-normal.csv'.format(dataset), 'r') as f:
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
            with open(args.dir + '/' + quota + '/average-{}-b-{}.csv'.format(dataset, b), 'r') as f:
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
    return {
        "dataset": dataset,
        "buffer_size": buffer_size,
        "buffer_size_strings": buffer_size_strings,
        "sage": sage,
        "queries": queries
    }

def plot_quotas(log=True, val = {}):
    dataset = val["dataset"]
    sage = val["sage"]
    fig, axes = plt.subplots(figsize=(14, 10), nrows=3, ncols=1, sharex=True)
    options = {
        "linestyle": "dashed",
        "marker": 'o',
        "markersize": 8
    }

    x = np.arange(len(sage))
    keys = ["150", "1500", "15000"]
    http = {
        "normal": [],
        0: [],
        100000: []
    }
    et = {
        "normal": [],
        0: [],
        100000: []
    }
    traffic = {
        "normal": [],
        0: [],
        100000: []
    }
    for h in sage["normal"]:
        http["normal"].append(h[0])
        et["normal"].append(h[1])
        traffic["normal"].append(h[2])
    for h in sage[0]:
        http[0].append(h[0])
        et[0].append(h[1])
        traffic[0].append(h[2])
    for h in sage[100000]:
        http[100000].append(h[0])
        et[100000].append(h[1])
        traffic[100000].append(h[2])
    plt.subplot(131)
    plt.plot(x, http["normal"], label="sage")
    plt.plot(x, http[0], label="sage-b-0")
    plt.plot(x, http[100000], label="sage-b-100KB")
    plt.xticks(x, labels=keys)
    plt.xlabel("Http requests for different Quotas (ms)")
    scale(log)
    plt.subplot(132)
    plt.plot(x, et["normal"], label="sage")
    plt.plot(x, et[0], label="sage-b-0")
    plt.plot(x, et[100000], label="sage-b-100KB")
    plt.xticks(x, labels=keys)
    plt.xlabel("Execution times in (ms) for different Quotas (ms)")
    scale(log)
    plt.subplot(133)
    plt.plot(x, traffic["normal"], label="sage")
    plt.plot(x, traffic[0], label="sage-b-0")
    plt.plot(x, traffic[100000], label="sage-b-100KB")
    plt.xticks(x, labels=keys)
    plt.xlabel("Traffic (in bytes) for different quotas (ms)")
    scale(log)
    plt.legend()
    plt.savefig(fname=args.o + 'quotas.png', quality=100, format='png', dpi=100)
    plt.close()



# plot_quotas()
