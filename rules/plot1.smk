####################################################################################################
####################################################################################################
########## PLOT 1: bsbm10 vs bsbm100 vs bsbm1k #####################################################
####################################################################################################
####################################################################################################

####################################################################################################
# >>>>> SAGE WITHOUT PARTIAL AGGREGATIONS ##########################################################
####################################################################################################

rule plot1_sage_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-performance/sage/{wcs.workload}/{wcs.dataset}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-performance/sage/{workload}/{dataset}/{run}/query_{query}.csv',
        result='output/data/bsbm-performance/sage/{workload}/{dataset}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['sage-exact-150ms']
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format xml 1> {output.result}'

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################
####################################################################################################

rule plot1_sage_agg_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-performance/sage-agg/{wcs.workload}/{wcs.dataset}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-performance/sage-agg/{workload}/{dataset}/{run}/query_{query}.csv',
        result='output/data/bsbm-performance/sage-agg/{workload}/{dataset}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['sage-exact-150ms']
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS + APPROXIMATIONS ############################################
####################################################################################################

rule plot1_sage_approx_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-performance/sage-approx/{wcs.workload}/{wcs.dataset}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-performance/sage-approx/{workload}/{dataset}/{run}/query_{query}.csv',
        result='output/data/bsbm-performance/sage-approx/{workload}/{dataset}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['sage-approx-98-150ms']
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> VIRTUOSO ###################################################################################
####################################################################################################

rule plot1_virtuoso_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-performance/virtuoso/{wcs.workload}/{wcs.dataset}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-performance/virtuoso/{workload}/{dataset}/{run}/query_{query}.csv',
        result='output/data/bsbm-performance/virtuoso/{workload}/{dataset}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['virtuoso']
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> COMUNICA ###################################################################################
####################################################################################################

rule plot1_comunica_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-performance/comunica/{wcs.workload}/{wcs.dataset}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-performance/comunica/{workload}/{dataset}/{run}/query_{query}.csv',
        result='output/data/bsbm-performance/comunica/{workload}/{dataset}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['ldf']
    shell:
        'node --max-old-space-size=6000 client/comunica/interface.js http://localhost:{params.port}/{wildcards.dataset} --file {input.query} --measure {output.stats} --format xml --output {output.result}'

####################################################################################################
# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################
####################################################################################################

rule plot1_format_query_file:
    input:
        ancient('output/data/bsbm-performance/{approach}/{workload}/{dataset}/{run}/query_{query}.csv')
    output:
        'output/data/bsbm-performance/{approach}/{workload}/{dataset}/{run}/mergeable_query_{query}.csv'
    params:
        label=lambda wcs: config["information"]["datasets_label"][wcs.dataset]
    shell:
        'touch {output}; '
        'echo "approach,workload,dataset,query,execution_time,nb_calls,data_transfer" >> {output}; '
        'echo -n "{wildcards.approach},{wildcards.workload},{params.label},Q{wildcards.query}," >> {output}; '
        'cat {input} >> {output}; '
        'echo "" >> {output};'


rule plot1_merge_query_files:
    input:
        expand('output/data/bsbm-performance/{{approach}}/{{workload}}/{{dataset}}/{{run}}/mergeable_query_{query}.csv', 
            query=config["settings"]["plot1"]["settings"]["queries"])
    output:
        'output/data/bsbm-performance/{approach}/{workload}/{dataset}/{run}/all.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot1_compute_average:
    input:
        expand('output/data/bsbm-performance/{{approach}}/{{workload}}/{{dataset}}/{run}/all.csv', 
            run=[x for x in range(1, last_run(1) + 1)])
    output:
        'output/data/bsbm-performance/{approach}/{workload}/{dataset}/data.csv'
    params:
        files=lambda wcs: [f'output/data/bsbm-performance/{wcs.approach}/{wcs.workload}/{wcs.dataset}/{run}/all.csv' for run in range(first_run(1), last_run(1) + 1)]
    shell:
        'python scripts/average.py {output} {params.files}'


rule plot1_merge_all_queries:
    input:
        expand('output/data/bsbm-performance/{{approach}}/{workload}/{dataset}/data.csv', 
            workload=config["settings"]["plot1"]["settings"]["workloads"], 
            dataset=config["settings"]["plot1"]["settings"]["datasets"])
    output:
        'output/data/bsbm-performance/{approach}/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot1_merge_all_approaches:
    input:
        expand('output/data/bsbm-performance/{approach}/data.csv', 
            approach=config["settings"]["plot1"]["settings"]["approaches"])
    output:
        'output/data/bsbm-performance/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule build_plot1:
    input:
        ancient('output/data/bsbm-performance/data.csv')
    output:
        'output/figures/bsbm_performance.png'
    shell:
        'python scripts/bsbm_plots.py --input {input} --output {output}'
