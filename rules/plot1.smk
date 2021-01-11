####################################################################################################
####################################################################################################
########## PLOT 1: bsbm10 vs bsbm100 vs bsbm1k #####################################################
####################################################################################################
####################################################################################################

PLOT1_DATASETS = ['bsbm10', 'bsbm100', 'bsbm1k']
PLOT1_APPROACHES = ['sage-agg']
PLOT1_WORKLOADS = ['SP', 'SP-ND']

SAGE_PORT = 8080
SAGE_APPROX_PORT = 8083
VIRTUOSO_PORT = 8890
LDF_PORT = 8000

####################################################################################################
# >>>>> SAGE WITHOUT PARTIAL AGGREGATIONS ##########################################################
####################################################################################################

rule plot1_sage_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage/{workload}/{dataset}/1/query_{query}.csv',
        result='output/data/performance/sage/{workload}/{dataset}/1/query_{query}.xml'
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format XML 1> {output.result}'


rule plot1_sage_second_run:
    input:
        first_run_complete=ancient('output/data/performance/sage/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage/{workload}/{dataset}/2/query_{query}.csv',
        result='output/data/performance/sage/{workload}/{dataset}/2/query_{query}.xml'
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format XML 1> {output.result}'


rule plot1_sage_third_run:
    input:
        second_run_complete=ancient('output/data/performance/sage/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage/{workload}/{dataset}/3/query_{query}.csv',
        result='output/data/performance/sage/{workload}/{dataset}/3/query_{query}.xml'
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format XML 1> {output.result}'

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################
####################################################################################################

rule plot1_sage_agg_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage-agg/{workload}/{dataset}/1/query_{query}.csv',
        result='output/data/performance/sage-agg/{workload}/{dataset}/1/query_{query}.xml'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT}/sparql http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot1_sage_agg_second_run:
    input:
        first_run_complete=ancient('output/data/performance/sage-agg/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage-agg/{workload}/{dataset}/2/query_{query}.csv',
        result='output/data/performance/sage-agg/{workload}/{dataset}/2/query_{query}.xml'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT}/sparql http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot1_sage_agg_third_run:
    input:
        second_run_complete=ancient('output/data/performance/sage-agg/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage-agg/{workload}/{dataset}/3/query_{query}.csv',
        result='output/data/performance/sage-agg/{workload}/{dataset}/3/query_{query}.xml'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT}/sparql http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS + APPROXIMATIONS ############################################
####################################################################################################

rule plot1_sage_approx_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage-approx/{workload}/{dataset}/1/query_{query}.csv',
        result='output/data/performance/sage-approx/{workload}/{dataset}/1/query_{query}.xml'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_APPROX_PORT}/sparql http://localhost:{SAGE_APPROX_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot1_sage_approx_second_run:
    input:
        first_run_complete=ancient('output/data/performance/sage-approx/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage-approx/{workload}/{dataset}/2/query_{query}.csv',
        result='output/data/performance/sage-approx/{workload}/{dataset}/2/query_{query}.xml'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_APPROX_PORT}/sparql http://localhost:{SAGE_APPROX_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot1_sage_approx_third_run:
    input:
        second_run_complete=ancient('output/data/performance/sage-approx/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/sage-approx/{workload}/{dataset}/3/query_{query}.csv',
        result='output/data/performance/sage-approx/{workload}/{dataset}/3/query_{query}.xml'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_APPROX_PORT}/sparql http://localhost:{SAGE_APPROX_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> VIRTUOSO ###################################################################################
####################################################################################################

rule plot1_virtuoso_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/virtuoso/{workload}/{dataset}/1/query_{query}.csv',
        result='output/data/performance/virtuoso/{workload}/{dataset}/1/query_{query}.xml'
    shell:
        'python client/virtuoso/interface.py query http://localhost:{VIRTUOSO_PORT}/sparql http://example.org/datasets/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot1_virtuoso_second_run:
    input:
        first_run_complete=ancient('output/data/performance/virtuoso/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/virtuoso/{workload}/{dataset}/2/query_{query}.csv',
        result='output/data/performance/virtuoso/{workload}/{dataset}/2/query_{query}.xml'
    shell:
        'python client/virtuoso/interface.py query http://localhost:{VIRTUOSO_PORT}/sparql http://example.org/datasets/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot1_virtuoso_third_run:
    input:
        second_run_complete=ancient('output/data/performance/virtuoso/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/virtuoso/{workload}/{dataset}/3/query_{query}.csv',
        result='output/data/performance/virtuoso/{workload}/{dataset}/3/query_{query}.xml'
    shell:
        'python client/virtuoso/interface.py query http://localhost:{VIRTUOSO_PORT}/sparql http://example.org/datasets/{wildcards.dataset} --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> COMUNICA ###################################################################################
####################################################################################################

rule plot1_comunica_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/comunica/{workload}/{dataset}/1/query_{query}.csv',
        result='output/data/performance/comunica/{workload}/{dataset}/1/query_{query}.xml'
    shell:
        'node --max-old-space-size=6000 client/comunica/interface.js http://localhost:{LDF_PORT}/{wildcards.dataset} --file {input} --measure {output.stats} --format xml --output {output.result}'


rule plot1_comunica_second_run:
    input:
        first_run_complete=ancient('output/data/performance/comunica/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/comunica/{workload}/{dataset}/2/query_{query}.csv',
        result='output/data/performance/comunica/{workload}/{dataset}/2/query_{query}.xml'
    shell:
        'node --max-old-space-size=6000 client/comunica/interface.js http://localhost:{LDF_PORT}/{wildcards.dataset} --file {input} --measure {output.stats} --format xml --output {output.result}'


rule plot1_comunica_third_run:
    input:
        second_run_complete=ancient('output/data/performance/comunica/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/performance/comunica/{workload}/{dataset}/3/query_{query}.csv',
        result='output/data/performance/comunica/{workload}/{dataset}/3/query_{query}.xml'
    shell:
        'node --max-old-space-size=6000 client/comunica/interface.js http://localhost:{LDF_PORT}/{wildcards.dataset} --file {input} --measure {output.stats} --format xml --output {output.result}'

# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################

rule plot1_format_query_file:
    input:
        ancient('output/data/performance/{approach}/{workload}/{dataset}/{run}/query_{query}.csv')
    output:
        'output/data/performance/{approach}/{workload}/{dataset}/{run}/mergeable_query_{query}.csv'
    shell:
        'touch {output}; '
        'echo "approach,workload,dataset,query,execution_time,nb_calls,data_transfer" >> {output}; '
        'echo -n "{wildcards.approach},{wildcards.workload},{wildcards.dataset},Q{wildcards.query}," >> {output}; '
        'cat {input} >> {output}; '
        'echo "" >> {output};'


rule plot1_merge_query_files:
    input:
        expand('output/data/performance/{{approach}}/{{workload}}/{{dataset}}/{{run}}/mergeable_query_{query}.csv', query=QUERIES)
    output:
        'output/data/performance/{approach}/{workload}/{dataset}/{run}/all.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot1_compute_average:
    input:
        expand('output/data/performance/{{approach}}/{{workload}}/{{dataset}}/{run}/all.csv', run=RUNS)
    output:
        'output/data/performance/{approach}/{workload}/{dataset}/all.csv'
    shell:
        'python scripts/average.py {input} {output}'


rule plot1_merge_all_files:
    input:
        expand('output/data/performance/{approach}/{workload}/{dataset}/all.csv', approach=PLOT1_APPROACHES, workload=PLOT1_WORKLOADS, dataset=PLOT1_DATASETS)
    output:
        'output/data/performance/bsbm_data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule build_plot1:
    input:
        ancient('output/data/performance/bsbm_data.csv')
    output:
        'output/figures/bsbm.png'
    shell:
        'python scripts/bsbm_plots.py --input {input} --output {output}'