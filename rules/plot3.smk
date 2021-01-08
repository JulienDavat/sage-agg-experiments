####################################################################################################
####################################################################################################
########## PLOT 3: Performance on a real dataset ###################################################
####################################################################################################
####################################################################################################

PLOT3_APPROACHES = ['sage-agg']
PLOT3_DATASETS = ['dbpedia']
PLOT3_WORKLOADS = ['SP', 'SP-ND']

# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################

# Information: this file mainly uses the rules defined in the file plot1.smk !!!

rule plot3_merge_all_files:
    input:
        expand('output/data/performance/{approach}/{workload}/{dataset}/all.csv', approach=PLOT3_APPROACHES, workload=PLOT3_WORKLOADS, dataset=PLOT3_DATASETS)
    output:
        'output/data/performance/dbpedia_data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'

rule build_plot3:
    input:
        ancient('output/data/performance/dbpedia_data.csv')
    output:
        'output/figures/dbpedia.png'
    shell:
        'python scripts/dbpedia_plots.py --input {input} --output {output}'