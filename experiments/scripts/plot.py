import csv
import argparse
import matplotlib.pyplot as plt
import numpy as np
import itertools
import os

parser = argparse.ArgumentParser(description='Plot an experiment, please provide a folder where results are located eg: output/second')
parser.add_argument('-dir', action='store', help='root dir of results, eg: output/')
# parser.add_argument('-queries', action='store', help='queries location')
parser.add_argument('-o', action='store', help='Output file')
args = parser.parse_args()

if args.dir is None or args.o is None:
    parser.print_help()
    parser.exit(1)

print("Output: ", args.o)

normal = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]
normal_wo_distinct = [1,19,20,21,22,6,23,24,25,26,11,27,28,14,30,31, 17, 18]

def process(toProcess = []):
    datasets = ['bsbm10', 'bsbm100', 'bsbm1k']
    datasets_labels = ['BSBM-10', 'BSBM-100', 'BSBM-1k']
    buffer_size = [0]
    buffer_size_strings = ["0"]

    tpf_dir = args.dir + "/tpf/"
    sage_dir = args.dir + "/sage/"
    virtuoso_dir = args.dir + "/virtuoso/"

    virtuoso = []
    sage = dict()
    tpf = []
    queries = dict()

    traffic_unit = 1000 * 1000 # 1000 = kb

    print(toProcess)
    for d in datasets:
        if os.path.exists(virtuoso_dir + 'average-{}.csv'.format(d)):
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
                        total_traffic += float(row[3]) / traffic_unit
                        if r not in queries:
                            queries[r] = dict()
                        if "virtuoso" not in queries[r]:
                            queries[r]["virtuoso"] = []
                        queries[r]["virtuoso"].append([float(row[1]), float(row[2]), float(row[3]) / traffic_unit])
                    r += 1
                virtuoso.append([total_http_requests, total_execution_time, total_traffic])
        if os.path.exists(sage_dir + 'average-{}-normal.csv'.format(d)):
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
                        total_traffic += t / traffic_unit
                        if r not in queries:
                            queries[r] = dict()
                        if "sage" not in queries[r]:
                            queries[r]["sage"] = dict()
                        if "normal" not in queries[r]["sage"]:
                            queries[r]["sage"]["normal"] = []
                        queries[r]["sage"]["normal"].append([float(row[1]), float(row[0]) * 1000, t / traffic_unit])
                    r += 1
                if "normal" not in sage:
                    sage["normal"] = []
                sage["normal"].append([total_http_requests, total_execution_time, total_traffic])

        for b in buffer_size:
            if os.path.exists(sage_dir + 'average-{}-b-{}.csv'.format(d, b)):
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
                            total_traffic += t / traffic_unit
                            if r not in queries:
                                queries[r] = dict()
                            if "sage" not in queries[r]:
                                queries[r]["sage"] = dict()
                            if b not in queries[r]["sage"]:
                                queries[r]["sage"][b] = []
                            queries[r]["sage"][b].append([float(row[1]), float(row[0]) * 1000, t / traffic_unit])
                        r += 1
                    if b not in sage:
                        sage[b] = []
                    sage[b].append([total_http_requests, total_execution_time, total_traffic])
        if os.path.exists(tpf_dir + 'average-{}.csv'.format(d)):
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
                        total_traffic += float(row[3]) / traffic_unit
                        if r not in queries:
                            queries[r] = dict()
                        if "tpf" not in queries[r]:
                            queries[r]["tpf"] = []
                        queries[r]["tpf"].append([float(row[2]), float(row[1]) * 1000, float(row[3]) / traffic_unit])
                    r += 1

                tpf.append([total_http_requests, total_execution_time, total_traffic])
    return {
        "datasets": datasets,
        "datasets_labels": datasets_labels,
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

def final(log=False):
    val = process(normal)
    datasets = val["datasets"]
    datasets_labels = val["datasets_labels"]
    virtuoso = val["virtuoso"]
    sage = val["sage"]
    tpf = val["tpf"]


    fig, axes = plt.subplots(figsize=(12, 8), nrows=2, ncols=2, sharex=True, sharey='row')
    print(axes)
    options = {
        "linestyle": "dashed",
        "markersize": 8
    }
    x = np.arange(3)
    lol = np.arange(len(datasets))

    markers = {
        "virtuoso": "+",
        "sage": "o",
        "sage-agg": "*",
        "tpf": "x"
    }

    ######### LEFT SIDE ###########

    # axes[0][0].plot(x, list(map(lambda e: e[0], virtuoso)), label="virtuoso", **options, marker=markers["virtuoso"])
    # axes[0][0].plot(x, list(map(lambda e: e[0], sage["normal"])), label="sage", **options, marker=markers["sage"])
    # axes[0][0].plot(x, list(map(lambda e: e[0], sage[0])), label="sage-agg", **options, marker=markers["sage-agg"])
    # axes[0][0].plot(x, list(map(lambda e: e[0], tpf)), label="tpf", **options, marker=markers["tpf"])
    # axes[0][0].set_xticks(x)
    # axes[0][0].set_xticklabels(datasets_labels)
    # axes[0][0].set_ylabel("HTTP Requests")
    # axes[0][0].set_yscale('log')

    ax = axes[0][0]
    ax.plot(x, list(map(lambda e: e[1], virtuoso)), label="virtuoso", **options, marker=markers["virtuoso"])
    ax.plot(x, list(map(lambda e: e[1], sage["normal"])), label="sage", **options, marker=markers["sage"])
    ax.plot(x, list(map(lambda e: e[1], sage[0])), label="sage-agg", **options, marker=markers["sage-agg"])
    ax.plot(x, list(map(lambda e: e[1], tpf)), label="tpf", **options, marker=markers["tpf"])
    ax.set_xticks(x)
    ax.set_xticklabels(datasets_labels)
    ax.set_ylabel("Execution Time (s)")
    ax.set_yscale('log')

    ax = axes[1][0]
    ax.plot(x, list(map(lambda e: e[2], virtuoso)), label="virtuoso", **options, marker=markers["virtuoso"])
    ax.plot(x, list(map(lambda e: e[2], sage["normal"])), label="sage", **options, marker=markers["sage"])
    ax.plot(x, list(map(lambda e: e[2], sage[0])), label="sage-agg", **options, marker=markers["sage-agg"])
    ax.plot(x, list(map(lambda e: e[2], tpf)), label="tpf", **options, marker=markers["tpf"])
    ax.set_xticks(x)
    ax.set_xticklabels(datasets_labels)
    ax.set_ylabel("Traffic (MBytes)")
    ax.set_yscale('log')

    ######### RIGHT SIDE ###########

    val = process(normal_wo_distinct)
    datasets = val["datasets"]
    virtuoso = val["virtuoso"]
    sage = val["sage"]
    tpf = val["tpf"]

    # axes[0][1].plot(x, list(map(lambda e: e[0], virtuoso)), label="virtuoso", **options, marker=markers["virtuoso"])
    # axes[0][1].plot(x, list(map(lambda e: e[0], sage["normal"])), label="sage", **options, marker=markers["sage"])
    # axes[0][1].plot(x, list(map(lambda e: e[0], sage[0])), label="sage-agg", **options, marker=markers["sage-agg"])
    # axes[0][1].plot(x, list(map(lambda e: e[0], tpf)), label="tpf", **options, marker=markers["tpf"])
    # axes[0][1].set_xticks(x)
    # axes[0][1].set_xticklabels(datasets_labels)
    # axes[0][1].set_yscale('log')
    # axes[0][1].yaxis.set_tick_params(which='both', labelbottom=True)

    ax = axes[0][1]
    ax.plot(x, list(map(lambda e: e[1], virtuoso)), label="virtuoso", **options, marker=markers["virtuoso"])
    ax.plot(x, list(map(lambda e: e[1], sage["normal"])), label="sage", **options, marker=markers["sage"])
    ax.plot(x, list(map(lambda e: e[1], sage[0])), label="sage-agg", **options, marker=markers["sage-agg"])
    ax.plot(x, list(map(lambda e: e[1], tpf)), label="tpf", **options, marker=markers["tpf"])
    ax.set_xticks(x)
    ax.set_xticklabels(datasets_labels)
    ax.set_yscale('log')
    ax.yaxis.set_tick_params(which='both', labelbottom=True)

    ax = axes[1][1]
    ax.plot(x, list(map(lambda e: e[2], virtuoso)), label="virtuoso", **options, marker=markers["virtuoso"])
    ax.plot(x, list(map(lambda e: e[2], sage["normal"])), label="sage", **options, marker=markers["sage"])
    ax.plot(x, list(map(lambda e: e[2], sage[0])), label="sage-agg", **options, marker=markers["sage-agg"])
    ax.plot(x, list(map(lambda e: e[2], tpf)), label="tpf", **options, marker=markers["tpf"])
    ax.set_xticks(x)
    ax.set_xticklabels(datasets_labels)
    ax.set_yscale('log')
    ax.yaxis.set_tick_params(which='both', labelbottom=True)

    plt.figlegend(('virtuoso', 'sage', 'sage-agg', 'tpf'), loc="upper center", shadow=True, ncol=4, bbox_to_anchor=(0.5, 0.94))
    plt.savefig(fname=args.o + 'plot-1.png', quality=100, format='png', dpi=100)
    plt.close()


def process_quotas(toProcess = [], traffic_unit = 1):
    quotas = ["150", "1500", "15000"]
    dataset = "bsbm1k"
    buffer_size = [0]
    buffer_size_strings = ["0"]

    main_dir = args.dir + ""

    virtuoso = []
    sage = dict()
    queries = dict()

    if os.path.exists(main_dir + '/virtuoso/average-{}.csv'.format('bsbm1k')):
        with open(main_dir + '/virtuoso/average-{}.csv'.format('bsbm1k'), 'r') as f:
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
                    total_traffic += float(row[3]) / traffic_unit
                    if r not in queries:
                        queries[r] = dict()
                    if "virtuoso" not in queries[r]:
                        queries[r]["virtuoso"] = []
                    queries[r]["virtuoso"].append([float(row[1]), float(row[2]), float(row[3]) / traffic_unit])
                r += 1
            virtuoso.append([total_http_requests, total_execution_time, total_traffic])

    for quota in quotas:
        with open(main_dir + '/' + quota + '/average-{}-normal.csv'.format(dataset), 'r') as f:
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
                    total_traffic += t / traffic_unit
                    if r not in queries:
                        queries[r] = dict()
                    if "sage" not in queries[r]:
                        queries[r]["sage"] = dict()
                    if "normal" not in queries[r]["sage"]:
                        queries[r]["sage"]["normal"] = []
                    queries[r]["sage"]["normal"].append([float(row[1]), float(row[0]) * 1000, t / traffic_unit])
                r += 1
            if "normal" not in sage:
                sage["normal"] = []
            sage["normal"].append([total_http_requests, total_execution_time, total_traffic])

        for b in buffer_size:
            with open(main_dir + '/' + quota + '/average-{}-b-{}.csv'.format(dataset, b), 'r') as f:
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
                        total_traffic += t / traffic_unit
                        if r not in queries:
                            queries[r] = dict()
                        if "sage" not in queries[r]:
                            queries[r]["sage"] = dict()
                        if b not in queries[r]["sage"]:
                            queries[r]["sage"][b] = []
                        queries[r]["sage"][b].append([float(row[1]), float(row[0]) * 1000, t / traffic_unit])
                    r += 1
                if b not in sage:
                    sage[b] = []
                sage[b].append([total_http_requests, total_execution_time, total_traffic])
    return {
        "dataset": dataset,
        "buffer_size": buffer_size,
        "buffer_size_strings": buffer_size_strings,
        "virtuoso": virtuoso,
        "sage": sage,
        "queries": queries
    }

def plot_quotas(log=True):
    traffic_unit = 1000 * 1000

    # ======== RIGHT SIDE ============

    fig, axes = plt.subplots(figsize=(10, 6), nrows=2, ncols=2, sharey='row')
    print(axes)
    options = {
        "linestyle": "dashed",
        "markersize": 8
    }

    markers = {
        "virtuoso": "+",
        "sage": "o",
        "sage-agg": "*",
        "tpf": "x"
    }

    x = np.arange(3)
    keys = ["150", "1500", "15000"]

    val = process_quotas(toProcess=[1,19,20,21,22,6,23,24,25,26,11,27,28,14,29,30, 17, 18], traffic_unit=traffic_unit)
    sage = val["sage"]
    virtuoso = val["virtuoso"]
    http = {
        "virtuoso": [],
        "normal": [],
        0: [],
    }
    et = {
        "virtuoso": [],
        "normal": [],
        0: [],
    }
    traffic = {
        "virtuoso": [],
        "normal": [],
        0: [],
    }

    for h in virtuoso:
        http["virtuoso"].append(h[0])
        et["virtuoso"].append(h[1])
        traffic["virtuoso"].append(h[2])
    for h in sage["normal"]:
        http["normal"].append(h[0])
        et["normal"].append(h[1])
        traffic["normal"].append(h[2])
    for h in sage[0]:
        http[0].append(h[0])
        et[0].append(h[1])
        traffic[0].append(h[2])

    print(et["virtuoso"], traffic['virtuoso'])
    ax = axes[0][1]
    ax.axhline(y=et["virtuoso"], label="virtuoso", color='#1f77b4', marker=markers["virtuoso"], **options)
    ax.plot(x, et["normal"], label="sage", color='#d62728', marker=markers["sage"], **options)
    ax.plot(x, et[0], label="sage-b-0", color='#2ca02c', marker=markers["sage-agg"], **options)
    ax.set_xticks(x)
    ax.set_xticklabels(labels=keys)
    ax.yaxis.set_tick_params(which='both', labelbottom=True)
    #ax.set_yscale('log')

    ax = axes[1][1]
    ax.axhline(y=traffic["virtuoso"], label="virtuoso", color='#1f77b4', marker=markers["virtuoso"], **options)
    ax.plot(x, traffic["normal"], label="sage", color='#d62728', marker=markers["sage"], **options)
    ax.plot(x, traffic[0], label="sage-b-0", color='#2ca02c', marker=markers["sage-agg"], **options)
    ax.set_xticks(x)
    ax.set_xticklabels(labels=keys)
    ax.yaxis.set_tick_params(which='both', labelbottom=True)
    #ax.set_yscale('log')



    val = process_quotas(toProcess=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18], traffic_unit=traffic_unit)
    sage = val["sage"]
    virtuoso = val["virtuoso"]
    http = {
        "virtuoso": [],
        "normal": [],
        0: [],
    }
    et = {
        "virtuoso": [],
        "normal": [],
        0: [],
    }
    traffic = {
        "virtuoso": [],
        "normal": [],
        0: [],
    }

    for h in virtuoso:
        http["virtuoso"].append(h[0])
        et["virtuoso"].append(h[1])
        traffic["virtuoso"].append(h[2])
    for h in sage["normal"]:
        http["normal"].append(h[0])
        et["normal"].append(h[1])
        traffic["normal"].append(h[2])
    for h in sage[0]:
        http[0].append(h[0])
        et[0].append(h[1])
        traffic[0].append(h[2])

    ax = axes[0][0]
    ax.axhline(y=et["virtuoso"], label="virtuoso", color='#1f77b4', marker=markers["virtuoso"], **options)
    ax.plot(x, et["normal"], label="sage", color='#d62728', marker=markers["sage"], **options)
    ax.plot(x, et[0], label="sage-b-0", color='green', marker=markers["sage-agg"], **options)
    ax.set_xticks(x)
    ax.set_xticklabels(labels=keys)
    #ax.set_yscale('log')
    ax.set_ylabel("Execution time (ms)")
    ax.yaxis.set_tick_params(which='both', labelbottom=True)

    ax = axes[1][0]
    ax.axhline(y=traffic["virtuoso"], label="virtuoso", color='#1f77b4', marker=markers["virtuoso"], **options)
    ax.plot(x, traffic["normal"], label="sage", color='#d62728', marker=markers["sage"], **options)
    ax.plot(x, traffic[0], label="sage-b-0", color='#2ca02c', marker=markers["sage-agg"], **options)
    ax.set_xticks(x)
    ax.set_xticklabels(labels=keys)
    #ax.set_yscale('log')
    ax.set_ylabel("Traffic (in MBytes)")
    ax.yaxis.set_tick_params(which='both', labelbottom=True)

    plt.figlegend(('virtuoso', 'sage', 'sage-agg'), loc="upper center", shadow=True, ncol=3, bbox_to_anchor=(0.5, 0.96))

    plt.savefig(fname=args.o + 'plot-2.png', quality=100, format='png', dpi=100)
    plt.close()


def plot_queries():
    sage_path = args.dir + '/sage/average-dbpedia351-b-0.csv'
    virtuoso_path = args.dir + '/virtuoso/average-dbpedia351.csv'
    sage_et = []
    sage_traffic = []
    traffic_unit = 1000 * 1000 # 1000 = kbytes
    et_unit = 1000 # 1 is ms
    with open(sage_path, 'r') as f:
        rows = csv.reader(f, delimiter=',')
        i = 1
        for row in rows:
            if i != 17:
                print(i, row)
                sage_et.append(float(row[0]) * 1000 / et_unit)
                sage_traffic.append(float(row[2]) / traffic_unit)
            i += 1

    virtuoso_et = []
    virtuoso_traffic = []
    with open(virtuoso_path, 'r') as f:
        rows = csv.reader(f, delimiter=',')
        i = 1
        includes = [1, 7, 11, 14, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
        print(len(includes))
        for row in rows:
            if i in includes:
                virtuoso_et.append(float(row[2]) / et_unit)
                virtuoso_traffic.append(float(row[3]) / traffic_unit)
            i += 1
    print(len(sage_et), sage_et, len(virtuoso_et), virtuoso_et)
    print(len(sage_traffic), sage_traffic, len(virtuoso_traffic), virtuoso_traffic)
    fig, axes = plt.subplots(figsize=(16, 4), nrows=1, ncols=2, sharey='col')
    width = 0.35
    x = np.arange(len(virtuoso_et))
    axes[0].axhline(60, color='red', label='virtuoso timeout')
    axes[0].bar(x - width/2, virtuoso_et, width, label='virtuoso')
    axes[0].bar(x + width/2, sage_et, width, label='sage-agg')
    axes[0].set_yscale('log')
    axes[0].set_xlabel("Execution time (in seconds)")

    axes[1].bar(x - width/2, virtuoso_traffic, width, label='virtuoso')
    axes[1].bar(x + width/2, sage_traffic, width, label='sage-agg')
    axes[1].set_yscale('log')
    axes[1].set_xlabel("Traffic (in MBytes)")

    axes[0].set_xticks(x)
    axes[1].set_xticks(x)

    axes[0].set_xticklabels(list(['Q{}'.format(a) for a in range(0, len(virtuoso_et))]))
    axes[1].set_xticklabels(list(['Q{}'.format(a) for a in range(0, len(virtuoso_et))]))
    plt.figlegend(('virtuoso timeout (60s)', 'virtuoso', 'sage-agg'), loc="upper center", shadow=True, ncol=3, bbox_to_anchor=(0.5, 1))
    plt.savefig(fname=args.o + 'plot-3.png', quality=100, format='png', dpi=100)
# final(log=True)
# plot_quotas()
plot_queries()