#!/bin/python3

import sys
import pandas as pd

def compute_average(files):
    data = list()
    for i in range(len(files)):
        df = pd.read_csv(files[i], sep=',')
        data.append(df)
    df = pd.concat(data)
    return df.groupby(['approach', 'workload', 'query']).mean()

if __name__ == '__main__':
    files = sys.argv[1:-1]
    output = sys.argv[-1]
    result = compute_average(files)
    with open(output, 'w') as out_file:
        out_file.write(result.to_csv())