####################################################################################################
####################################################################################################
########## PLOT 3: Performance on a real dataset ###################################################
####################################################################################################
####################################################################################################

####################################################################################################
# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################
####################################################################################################

# Information: this file mainly uses the rules defined in the file plot1.smk !!!

rule plot3_compute_average:
    input:
        expand('output/data/performance/{{approach}}/{{workload}}/{{dataset}}/{run}/all.csv', 
            run=[x for x in range(1, last_run(3) + 1)])
    output:
        'output/data/performance/{approach}/{workload}/{dataset}/all-plot3.csv'
    params:
        files=lambda wcs: [f'output/data/performance/{wcs.approach}/{wcs.workload}/{wcs.dataset}/{run}/all.csv' for run in range(first_run(3), last_run(3) + 1)]
    shell:
        'python scripts/average.py {output} "approach,workload,dataset,query" {params.files}'

rule plot3_merge_all_files:
    input:
        expand('output/data/performance/{approach}/{workload}/{dataset}/all-plot3.csv', 
            approach=config["settings"]["plot3"]["settings"]["approaches"], 
            workload=config["settings"]["plot3"]["settings"]["workloads"], 
            dataset=config["settings"]["plot3"]["settings"]["datasets"])
    output:
        'output/data/plot3.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'

rule build_plot3:
    input:
        ancient('output/data/plot3.csv')
    output:
        'output/figures/dbpedia.png'
    shell:
        'python scripts/dbpedia_plots.py --input {input} --output {output}'