#!/bin/python3

import sys
import pandas as pd

def compute_average(files, group_by_variables):
    data = list()
    for i in range(len(files)):
        df = pd.read_csv(files[i], sep=',')
        data.append(df)
    df = pd.concat(data)
    return df.groupby(group_by_variables).mean()

if __name__ == '__main__':
    output = sys.argv[1]
    group_by_variables = sys.argv[2].split(',')
    files = sys.argv[3:]
    result = compute_average(files, group_by_variables)
    with open(output, 'w') as out_file:
        out_file.write(result.to_csv())