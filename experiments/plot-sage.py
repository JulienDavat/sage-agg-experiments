import matplotlib.pyplot as plt
import numpy as np
import csv
import argparse
import glob
import os
import functools

from utils import plotSage, multiPlotSageNormalAgg

parser = argparse.ArgumentParser(description='Plot an experiment using virtuoso bash file ')
parser.add_argument('path', metavar='path', nargs='+', help='Path to the experiment')
# parser.add_argument('--sage', help='Use if it is a sage experiment', action="store_true")
args = parser.parse_args()

experiments = []
labels = []
pathes = []

for path in args.path:
    print('Reading %s' % path)
    labels = plotSage(path)
    pathes.append(path)
    with open(path + '/sage.csv', 'r') as f:
        rows = csv.reader(f, delimiter=',')
        val = []
        for row in rows:
            val.append(row)
        experiments.append(val)

multiPlotSageNormalAgg(experiments[0], experiments[1], labels)