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
        stats='output/data/quantum/sage/{workload}/{quantum}/1/query_{query}.csv',
        result='output/data/quantum/sage/{workload}/{quantum}/1/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage'][wcs.quantum]
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format XML 1> {output.result}'


rule plot2_sage_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/sage/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage/{workload}/{quantum}/2/query_{query}.csv',
        result='output/data/quantum/sage/{workload}/{quantum}/2/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage'][wcs.quantum]
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format XML 1> {output.result}'


rule plot2_sage_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/sage/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage/{workload}/{quantum}/3/query_{query}.csv',
        result='output/data/quantum/sage/{workload}/{quantum}/3/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage'][wcs.quantum]
    shell:
        'java -Xmx6g -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format XML 1> {output.result}'

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################
####################################################################################################

rule plot2_sage_agg_first_run:
    input: 
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage-agg/{workload}/{quantum}/1/query_{query}.csv',
        result='output/data/quantum/sage-agg/{workload}/{quantum}/1/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-agg'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot2_sage_agg_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/sage-agg/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage-agg/{workload}/{quantum}/2/query_{query}.csv',
        result='output/data/quantum/sage-agg/{workload}/{quantum}/2/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-agg'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot2_sage_agg_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/sage-agg/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage-agg/{workload}/{quantum}/3/query_{query}.csv',
        result='output/data/quantum/sage-agg/{workload}/{quantum}/3/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-agg'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> SAGE WITH PARTIAL AGGREGATIONS + APPROXIMATIONS ############################################
####################################################################################################

rule plot2_sage_approx_first_run:
    input: 
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage-approx/{workload}/{quantum}/1/query_{query}.csv',
        result='output/data/quantum/sage-approx/{workload}/{quantum}/1/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-approx'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot2_sage_approx_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/sage-approx/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage-approx/{workload}/{quantum}/2/query_{query}.csv',
        result='output/data/quantum/sage-approx/{workload}/{quantum}/2/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-approx'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot2_sage_approx_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/sage-approx/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/sage-approx/{workload}/{quantum}/3/query_{query}.csv',
        result='output/data/quantum/sage-approx/{workload}/{quantum}/3/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['sage-approx'][wcs.quantum]
    shell:
        'python client/sage-agg/interface.py query http://localhost:{params.port}/sparql http://localhost:{params.port}/sparql/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

####################################################################################################
# >>>>> VIRTUOSO ###################################################################################
####################################################################################################

rule plot2_virtuoso_first_run:
    input: 
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/virtuoso/{workload}/{quantum}/1/query_{query}.csv',
        result='output/data/quantum/virtuoso/{workload}/{quantum}/1/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['virtuoso'][wcs.quantum]
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot2_virtuoso_second_run:
    input:
        first_run_complete=ancient('output/data/quantum/virtuoso/{workload}/{quantum}/1/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/virtuoso/{workload}/{quantum}/2/query_{query}.csv',
        result='output/data/quantum/virtuoso/{workload}/{quantum}/2/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['virtuoso'][wcs.quantum]
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '


rule plot2_virtuoso_third_run:
    input:
        second_run_complete=ancient('output/data/quantum/virtuoso/{workload}/{quantum}/2/all.csv'),
        graph=ancient('graphs/bsbm1k.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        stats='output/data/quantum/virtuoso/{workload}/{quantum}/3/query_{query}.csv',
        result='output/data/quantum/virtuoso/{workload}/{quantum}/3/query_{query}.xml'
    params:
        port=lambda wcs: QUANTUM_TO_PORT['virtuoso'][wcs.quantum]
    shell:
        'python client/virtuoso/interface.py query http://localhost:{params.port}/sparql http://example.org/datasets/bsbm1k --file {input.query} --measure {output.stats} --format w3c/xml --output {output.result}; '

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
        expand('output/data/quantum/{{approach}}/{{workload}}/{{quantum}}/{run}/all.csv', run=RUNS)
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


rule build_plot2:
    input:
        ancient('output/data/quantum/data.csv')
    output:
        'output/figures/quantum_impact.png'
    shell:
        'python scripts/quantum_plots.py --input {input} --output {output}'