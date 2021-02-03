####################################################################################################
####################################################################################################
########## PLOT 2: Impact of the quantum ###########################################################
####################################################################################################
####################################################################################################

####################################################################################################
# >>>>> SAGE WITHOUT PARTIAL AGGREGATIONS ##########################################################
####################################################################################################

rule plot2_sage_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-quantum/sage/{wcs.workload}/{wcs.quantum}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-quantum/sage/{workload}/{quantum}/{run}/query_{query}.csv',
        result='output/data/bsbm-quantum/sage/{workload}/{quantum}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"][f'sage-exact-{wcs.quantum}ms']
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format xml 1> {output.result}'

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################
####################################################################################################

rule plot2_sage_agg_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-quantum/sage-agg/{wcs.workload}/{wcs.quantum}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-quantum/sage-agg/{workload}/{quantum}/{run}/query_{query}.csv',
        result='output/data/bsbm-quantum/sage-agg/{workload}/{quantum}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"][f'sage-exact-{wcs.quantum}ms']
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS + APPROXIMATIONS ############################################
####################################################################################################

rule plot2_sage_approx_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-quantum/sage-approx/{wcs.workload}/{wcs.quantum}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-quantum/sage-approx/{workload}/{quantum}/{run}/query_{query}.csv',
        result='output/data/bsbm-quantum/sage-approx/{workload}/{quantum}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"][f'sage-approx-98-{wcs.quantum}ms']
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> VIRTUOSO ###################################################################################
####################################################################################################

rule plot2_virtuoso_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/bsbm-quantum/virtuoso/{wcs.workload}/{wcs.quantum}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/bsbm-quantum/virtuoso/{workload}/{quantum}/{run}/query_{query}.csv',
        result='output/data/bsbm-quantum/virtuoso/{workload}/{quantum}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['virtuoso']
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################
####################################################################################################

rule plot2_format_query_file:
    input:
        ancient('output/data/bsbm-quantum/{approach}/{workload}/{quantum}/{run}/query_{query}.csv')
    output:
        'output/data/bsbm-quantum/{approach}/{workload}/{quantum}/{run}/mergeable_query_{query}.csv'
    shell:
        'touch {output}; '
        'echo "approach,workload,quantum,query,execution_time,nb_calls,data_transfer" >> {output}; '
        'echo -n "{wildcards.approach},{wildcards.workload},{wildcards.quantum},Q{wildcards.query}," >> {output}; '
        'cat {input} >> {output}; '
        'echo "" >> {output};'


rule plot2_merge_query_files:
    input:
        expand('output/data/bsbm-quantum/{{approach}}/{{workload}}/{{quantum}}/{{run}}/mergeable_query_{query}.csv', 
            query=config["settings"]["plot2"]["settings"]["queries"])
    output:
        'output/data/bsbm-quantum/{approach}/{workload}/{quantum}/{run}/all.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot2_compute_average:
    input:
        expand('output/data/bsbm-quantum/{{approach}}/{{workload}}/{{quantum}}/{run}/all.csv', 
            run=[x for x in range(1, last_run(2) + 1)])
    output:
        'output/data/bsbm-quantum/{approach}/{workload}/{quantum}/data.csv'
    params:
        files=lambda wcs: [f'output/data/bsbm-quantum/{wcs.approach}/{wcs.workload}/{wcs.quantum}/{run}/all.csv' for run in range(first_run(2), last_run(2) + 1)]
    shell:
        'python scripts/average.py {output} {params.files}'


rule plot2_merge_all_queries:
    input:
        expand('output/data/bsbm-quantum/{{approach}}/{workload}/{quantum}/data.csv', 
            workload=config["settings"]["plot2"]["settings"]["workloads"], 
            quantum=config["settings"]["plot2"]["settings"]["quantums"])
    output:
        'output/data/bsbm-quantum/{approach}/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot2_merge_all_approaches:
    input:
        expand('output/data/bsbm-quantum/{approach}/data.csv', 
            approach=config["settings"]["plot2"]["settings"]["approaches"])
    output:
        'output/data/bsbm-quantum/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule build_plot2:
    input:
        ancient('output/data/bsbm-quantum/data.csv')
    output:
        'output/figures/quantum_impacts.png'
    shell:
        'python scripts/quantum_plots.py --input {input} --output {output}'