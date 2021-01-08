# QUERIES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
QUERIES = [1, 2, 3]
RUNS = [1, 2, 3]

# rule run_all:
#     input: expand('queries/{{workload}}/query_{query}.sparql', query=QUERIES)

####################################################################################################
####################################################################################################
########## INITIALIZATION ##########################################################################
####################################################################################################
####################################################################################################

onstart:
    shell('mkdir -p output/log')
    shell('bash scripts/start_sage_servers.sh')
    print('starting sage servers...')

onsuccess:
    shell('bash scripts/stop_sage_servers.sh')
    print('Shutting down sage servers')

onerror:
    shell('bash scripts/stop_sage_servers.sh')
    print('Shutting down sage servers')

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
        'output/data/performance/sage/{workload}/{dataset}/1/query_{query}.csv'
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output} --time'


rule plot1_sage_second_run:
    input:
        first_run_complete=ancient('output/data/performance/sage/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage/{workload}/{dataset}/2/query_{query}.csv'
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output} --time'


rule plot1_sage_third_run:
    input:
        second_run_complete=ancient('output/data/performance/sage/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage/{workload}/{dataset}/3/query_{query}.csv'
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output} --time'

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################
####################################################################################################

rule plot1_sage_agg_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage-agg/{workload}/{dataset}/1/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT}/sparql http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '


rule plot1_sage_agg_second_run:
    input:
        first_run_complete=ancient('output/data/performance/sage-agg/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage-agg/{workload}/{dataset}/2/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT}/sparql http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '


rule plot1_sage_agg_third_run:
    input:
        second_run_complete=ancient('output/data/performance/sage-agg/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage-agg/{workload}/{dataset}/3/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT}/sparql http://localhost:{SAGE_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS + APPROXIMATIONS ############################################
####################################################################################################

rule plot1_sage_approx_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage-approx/{workload}/{dataset}/1/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_APPROX_PORT}/sparql http://localhost:{SAGE_APPROX_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '


rule plot1_sage_approx_second_run:
    input:
        first_run_complete=ancient('output/data/performance/sage-approx/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage-approx/{workload}/{dataset}/2/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_APPROX_PORT}/sparql http://localhost:{SAGE_APPROX_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '


rule plot1_sage_approx_third_run:
    input:
        second_run_complete=ancient('output/data/performance/sage-approx/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/sage-approx/{workload}/{dataset}/3/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_APPROX_PORT}/sparql http://localhost:{SAGE_APPROX_PORT}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '

####################################################################################################
# >>>>> VIRTUOSO ###################################################################################
####################################################################################################

rule plot1_virtuoso_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/virtuoso/{workload}/{dataset}/1/query_{query}.csv'
    shell:
        'python client/virtuoso/interface.py query http://localhost:{VIRTUOSO_PORT}/sparql http://example.org/datasets/{wildcards.dataset} --file {input.query} --measure {output}; '


rule plot1_virtuoso_second_run:
    input:
        first_run_complete=ancient('output/data/performance/virtuoso/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/virtuoso/{workload}/{dataset}/2/query_{query}.csv'
    shell:
        'python client/virtuoso/interface.py query http://localhost:{VIRTUOSO_PORT}/sparql http://example.org/datasets/{wildcards.dataset} --file {input.query} --measure {output}; '


rule plot1_virtuoso_third_run:
    input:
        second_run_complete=ancient('output/data/performance/virtuoso/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/virtuoso/{workload}/{dataset}/3/query_{query}.csv'
    shell:
        'python client/virtuoso/interface.py query http://localhost:{VIRTUOSO_PORT}/sparql http://example.org/datasets/{wildcards.dataset} --file {input.query} --measure {output}; '

####################################################################################################
# >>>>> COMUNICA ###################################################################################
####################################################################################################

rule plot1_comunica_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/comunica/{workload}/{dataset}/1/query_{query}.csv'
    shell:
        'node --max-old-space-size=6000 client/comunica/comunica.js http://localhost:{LDF_PORT}/{wildcards.dataset} --file {input} --measure {output}'


rule plot1_comunica_second_run:
    input:
        first_run_complete=ancient('output/data/performance/comunica/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/comunica/{workload}/{dataset}/2/query_{query}.csv'
    shell:
        'node --max-old-space-size=6000 client/comunica/comunica.js http://localhost:{LDF_PORT}/{wildcards.dataset} --file {input} --measure {output}'


rule plot1_comunica_third_run:
    input:
        second_run_complete=ancient('output/data/performance/comunica/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/performance/comunica/{workload}/{dataset}/3/query_{query}.csv'
    shell:
        'node --max-old-space-size=6000 client/comunica/comunica.js http://localhost:{LDF_PORT}/{wildcards.dataset} --file {input} --measure {output}'

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
        ancient('output/data/performance/{approach}/{workload}/{dataset}/1/all.csv'),
        ancient('output/data/performance/{approach}/{workload}/{dataset}/2/all.csv'),
        ancient('output/data/performance/{approach}/{workload}/{dataset}/3/all.csv')
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

####################################################################################################
####################################################################################################
########## PLOT 2: Impact of the quantum ###########################################################
####################################################################################################
####################################################################################################

PLOT2_QUANTUMS = ['150', '1500', '15000']
PLOT2_APPROACHES = ['sage-agg']
PLOT2_WORKLOADS = ['SP', 'SP-ND']

QUANTUM_TO_PORT = {
    'sage': {'150': '8080', '1500': '8081', '15000': '8082'},
    'sage-agg': {'150': '8080', '1500': '8081', '15000': '8082'},
    'sage-approx': {'150': '8083', '1500': '8084', '15000': '8085'},
    'virtuoso': {'150': '8890', '1500': '8890', '15000': '8890'}
}

####################################################################################################
# >>>>> SAGE WITHOUT PARTIAL AGGREGATIONS ##########################################################
####################################################################################################

rule plot2_sage_first_run:
    input: 
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage/{workload}/{quantum}/1/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage'][wcs.quantum]
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output} --time'


rule plot2_sage_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/sage/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage/{workload}/{quantum}/2/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage'][wcs.quantum]
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output} --time'


rule plot2_sage_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/sage/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage/{workload}/{quantum}/3/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage'][wcs.quantum]
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output} --time'

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################
####################################################################################################

rule plot2_sage_agg_first_run:
    input: 
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage-agg/{workload}/{quantum}/1/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-agg'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output}; '


rule plot2_sage_agg_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/sage-agg/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage-agg/{workload}/{quantum}/2/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-agg'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output}; '


rule plot2_sage_agg_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/sage-agg/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage-agg/{workload}/{quantum}/3/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-agg'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output}; '

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS + APPROXIMATIONS ############################################
####################################################################################################

rule plot2_sage_approx_first_run:
    input: 
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage-approx/{workload}/{quantum}/1/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-approx'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output}; '


rule plot2_sage_approx_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/sage-approx/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage-approx/{workload}/{quantum}/2/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-approx'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output}; '


rule plot2_sage_approx_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/sage-approx/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/sage-approx/{workload}/{quantum}/3/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-approx'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output}; '

####################################################################################################
# >>>>> VIRTUOSO ###################################################################################
####################################################################################################

rule plot2_virtuoso_first_run:
    input: 
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/virtuoso/{workload}/{quantum}/1/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['virtuoso'][wcs.quantum]
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output}; '


rule plot2_virtuoso_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/virtuoso/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/virtuoso/{workload}/{quantum}/2/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['virtuoso'][wcs.quantum]
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output}; '


rule plot2_virtuoso_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/virtuoso/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/quantum/virtuoso/{workload}/{quantum}/3/query_{query}.csv'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['virtuoso'][wcs.quantum]
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output}; '

# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################

rule plot2_format_query_file:
    input:
        ancient('output/data/quantum/{approach}/{workload}/{quantum}/{run}/query_{query}.csv')
    output:
        'output/data/quantum/{approach}/{workload}/{quantum}/{run}/mergeable_query_{query}.csv'
    shell:
        'touch {output}; '
        'echo "approach,workload,quantum,query,execution_time,nb_calls,data_transfer" >> {output}; '
        'echo -n "{wildcards.approach},{wildcards.workload},{wildcards.quantum},Q{wildcards.query}," >> {output}; '
        'cat {input} >> {output}; '
        'echo "" >> {output};'


rule plot2_merge_query_files:
    input:
        expand('output/data/quantum/{{approach}}/{{workload}}/{{quantum}}/{{run}}/mergeable_query_{query}.csv', query=QUERIES)
    output:
        'output/data/quantum/{approach}/{workload}/{quantum}/{run}/all.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule plot2_compute_average:
    input:
        ancient('output/data/quantum/{approach}/{workload}/{quantum}/1/all.csv'),
        ancient('output/data/quantum/{approach}/{workload}/{quantum}/2/all.csv'),
        ancient('output/data/quantum/{approach}/{workload}/{quantum}/3/all.csv')
    output:
        'output/data/quantum/{approach}/{workload}/{quantum}/all.csv'
    shell:
        'python scripts/average.py {input} {output}'


rule plot2_merge_all_files:
    input:
        expand('output/data/quantum/{approach}/{workload}/{quantum}/all.csv', approach=PLOT2_APPROACHES, workload=PLOT2_WORKLOADS, quantum=PLOT2_QUANTUMS)
    output:
        'output/data/quantum/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'

####################################################################################################
####################################################################################################
########## PLOT 3: Performance on a real dataset ###################################################
####################################################################################################
####################################################################################################

PLOT3_APPROACHES = ['sage-agg']
PLOT3_DATASETS = ['dbpedia']
PLOT3_WORKLOADS = ['SP', 'SP-ND']

# >>>>> PREPARE CSV FILES TO BUILD PLOTS ###########################################################

rule plot3_merge_all_files:
    input:
        expand('output/data/performance/{approach}/{workload}/{dataset}/all.csv', approach=PLOT3_APPROACHES, workload=PLOT3_WORKLOADS, dataset=PLOT3_DATASETS)
    output:
        'output/data/performance/dbpedia_data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'