####################################################################################################
####################################################################################################
########## PLOT 2: Impact of the precision ###########################################################
####################################################################################################
####################################################################################################

####################################################################################################
# >>>>> SAGE WITHOUT PARTIAL AGGREGATIONS ##########################################################
####################################################################################################

rule plot4_sage_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/precision/sage/{wcs.workload}/{wcs.precision}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/precision/sage/{workload}/{precision}/{run}/query_{query}.csv',
        result='output/data/precision/sage/{workload}/{precision}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['sage-exact-150ms']
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format xml 1> {output.result}'

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################
####################################################################################################

rule plot4_sage_agg_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/precision/sage-agg/{wcs.workload}/{wcs.precision}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/precision/sage-agg/{workload}/{precision}/{run}/query_{query}.csv',
        result='output/data/precision/sage-agg/{workload}/{precision}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['sage-exact-150ms']
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS + APPROXIMATIONS ############################################
####################################################################################################

rule plot4_sage_approx_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/precision/sage-approx/{wcs.workload}/{wcs.precision}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/precision/sage-approx/{workload}/{precision}/{run}/query_{query}.csv',
        result='output/data/precision/sage-approx/{workload}/{precision}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"][f'sage-approx-{wcs.precision}-150ms']
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> VIRTUOSO ###################################################################################
####################################################################################################

rule plot4_virtuoso_run:
    input: 
        previous_run_complete=lambda wcs: [] if int(wcs.run) == 1 else [f'output/data/precision/virtuoso/{wcs.workload}/{wcs.precision}/{int(wcs.run) - 1}/all.csv'],
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/precision/virtuoso/{workload}/{precision}/{run}/query_{query}.csv',
        result='output/data/precision/virtuoso/{workload}/{precision}/{run}/query_{query}.xml'
    params:
        port=lambda wcs: config["information"]["ports"]['virtuoso']
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################
####################################################################################################

rule plot4_format_query_file:
    input:
        ancient('output/data/bsbm-precision/{approach}/{workload}/{precision}/{run}/query_{query}.csv')
    output:
        'output/data/bsbm-precision/{approach}/{workload}/{precision}/{run}/mergeable_query_{query}.csv'
    shell:
        'touch {output}; '
        'echo "approach,workload,precision,query,execution_time,nb_calls,data_transfer" >> {output}; '
        'echo -n "{wildcards.approach},{wildcards.workload},{wildcards.precision},Q{wildcards.query}," >> {output}; '
        'cat {input} >> {output}; '
        'echo "" >> {output};'


rule plot4_merge_query_files:
    input:
        expand('output/data/bsbm-precision/{{approach}}/{{workload}}/{{precision}}/{{run}}/mergeable_query_{query}.csv', 
            query=config["settings"]["plot4"]["settings"]["queries"])
    output:
        'output/data/bsbm-precision/{approach}/{workload}/{precision}/{run}/all.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot4_compute_average:
    input:
        expand('output/data/bsbm-precision/{{approach}}/{{workload}}/{{precision}}/{run}/all.csv', 
            run=[x for x in range(1, last_run(4) + 1)])
    output:
        'output/data/bsbm-precision/{approach}/{workload}/{precision}/data.csv'
    params:
        files=lambda wcs: [f'output/data/bsbm-precision/{wcs.approach}/{wcs.workload}/{wcs.precision}/{run}/all.csv' for run in range(first_run(4), last_run(4) + 1)]
    shell:
        'python scripts/average.py {output} {params.files}'


rule plot4_merge_all_queries:
    input:
        expand('output/data/bsbm-precision/{{approach}}/{workload}/{precision}/data.csv', 
            workload=config["settings"]["plot4"]["settings"]["workloads"], 
            precision=config["settings"]["plot4"]["settings"]["precisions"])
    output:
        'output/data/bsbm-precision/{approach}/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot4_merge_all_approaches:
    input:
        expand('output/data/bsbm-precision/{approach}/data.csv', 
            approach=config["settings"]["plot4"]["settings"]["approaches"])
    output:
        'output/data/bsbm-precision/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule build_plot4:
    input:
        ancient('output/data/bsbm-precision/data.csv')
    output:
        'output/figures/precision_impacts.png'
    shell:
        'python scripts/precision_plots.py --input {input} --output {output}'