from argparse import ArgumentParser, ArgumentTypeError
from seaborn import lineplot
from pandas import read_csv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ====================================================================================================
# ===== Command line interface =======================================================================
# ====================================================================================================

parser = ArgumentParser()
parser.add_argument("--input", "-i", help="The file that contains queries evaluation statistics", default=None)
parser.add_argument("--output", "-o", help="The path of the file in which the figure will be saved", default=None)
args = parser.parse_args()

input_file = args.input
output_file = args.output

if input_file is None or output_file is None:
    print('Error: missing required arguments ! USAGE: quantum_plots.py --input <file> --output <file>')
    exit(1)

# ====================================================================================================
# ===== Figures construction =========================================================================
# ====================================================================================================

def transform_ms_to_sec(data):
    data['execution_time'] = data['execution_time'].div(1000)

def transform_bytes_to_kbytes(data):
    data['data_transfer'] = data['data_transfer'].div(1024)

def plot_metric(ax, data, metric, title, xlabel, ylabel, logscale=False, display_x=True):
    df = data.copy()
    df['quantum'] = pd.Categorical(df['quantum'], categories=['75','150','1500','15000'], ordered=True)
    df = df[['approach', 'quantum', metric]].groupby(['approach', 'quantum']).sum()
    
    dashes = [(2,2)] * data['approach'].nunique()
    chart = lineplot(x='quantum', y=metric, hue='approach', data=df, ax=ax, markers=True, style="approach", dashes=dashes)
    if logscale:
        chart.set_yscale("log")
    chart.set_title(title)
    chart.set_xlabel(xlabel)
    chart.set_ylabel(ylabel)
    chart.legend().set_title('')
    if not display_x:
        chart.set(xticklabels=[])

def create_figure(data, logscale=False):
    data['quantum'] = data['quantum'].astype(str)
    sp_workload = data[(data['workload'] == 'SP')]
    sp_nd_workload = data[(data['workload'] == 'SP-ND')]
    # initialization of the figure
    fig = plt.figure(figsize=(8, 6))
    plt.subplots_adjust(hspace=0.2)
    # creation of the left part (SP workload)
    ax1 = fig.add_subplot(221)
    plot_metric(ax1, sp_workload, 'execution_time', '', '', 'Execution Time (sec)', logscale=logscale, display_x=False)
    plt.legend().remove()
    fig.legend(loc='upper center', bbox_to_anchor=(0.5, 0.97), fancybox=True, shadow=True, ncol=5)
    ax2 = fig.add_subplot(223)
    plot_metric(ax2, sp_workload, 'data_transfer', '', '', 'Traffic (KBytes)', logscale=logscale)
    plt.legend().remove()
    # creattion of the right part (SP-ND workload)
    ax3 = fig.add_subplot(222)
    plot_metric(ax3, sp_nd_workload, 'execution_time', '', '', '', logscale=logscale, display_x=False)
    plt.legend().remove()
    ax4 = fig.add_subplot(224)
    plot_metric(ax4, sp_nd_workload, 'data_transfer', '', '', '', logscale=logscale)
    plt.legend().remove()
    
    plt.show()
    return fig

dataframe = read_csv(input_file, sep=',')
print(dataframe)
transform_bytes_to_kbytes(dataframe)

figure = create_figure(dataframe, logscale=True)
figure.savefig(output_file)
