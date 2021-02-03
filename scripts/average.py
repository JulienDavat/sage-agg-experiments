#!/bin/python3

import sys
import pandas as pd

def compute_average(files):
    data = list()
    for i in range(len(files)):
        df = pd.read_csv(files[i], sep=',')
        data.append(df)
    df = pd.concat(data)

    metrics = [
        'execution_time',
        'nb_calls', 
        'data_transfer'
    ]
    group_by_variables = [column for column in list(df.columns) if column not in metrics]

    return df.groupby(group_by_variables).mean()

if __name__ == '__main__':
    output = sys.argv[1]
    files = sys.argv[2:]
    result = compute_average(files)
    with open(output, 'w') as out_file:
        out_file.write(result.to_csv())