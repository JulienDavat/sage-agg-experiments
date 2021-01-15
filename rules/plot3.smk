####################################################################################################
####################################################################################################
########## PLOT 3: Performance on a real dataset ###################################################
####################################################################################################
####################################################################################################

####################################################################################################
# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################
####################################################################################################

# Information: this file mainly uses the rules defined in the file plot1.smk !!!

rule plot3_merge_all_files:
    input:
        expand('output/data/performance/{approach}/{workload}/{dataset}/all.csv', 
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