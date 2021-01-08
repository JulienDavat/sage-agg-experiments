# QUERIES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
QUERIES = [1, 2, 3]
BSBM_DATASETS = ['bsbm10', 'bsbm100', 'bsbm1k']

SAGE_PORT_150MS=8080
SAGE_PORT_1500MS=8081
SAGE_PORT_15000MS=8082
SAGE_APPROX_PORT_150MS=8083

# rule run_all:
#     input: expand('queries/{{workload}}/query_{query}.sparql', query=QUERIES)


####################################################################################################
########## Initialization ##########################################################################
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
########## PLOT 1: bsbm10 vs bsbm100 vs bsbm1k #####################################################
####################################################################################################

# >>>>> SAGE WITH PARTIAL AGGREGATIONS #############################################################

rule sage_agg_performance_first_run:
    input: 
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/sage-agg/performance/{workload}/{dataset}/1/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT_150MS}/sparql http://localhost:{SAGE_PORT_150MS}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '


rule sage_agg_performance_second_run:
    input:
        first_run_complete=ancient('output/data/sage-agg/performance/{workload}/{dataset}/1/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/sage-agg/performance/{workload}/{dataset}/2/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT_150MS}/sparql http://localhost:{SAGE_PORT_150MS}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '


rule sage_agg_performance_third_run:
    input:
        second_run_complete=ancient('output/data/sage-agg/performance/{workload}/{dataset}/2/all.csv'),
        graph=ancient('graphs/{dataset}.nt'),
        query=ancient('queries/{workload}/query_{query}.sparql')
    output:
        'output/data/sage-agg/performance/{workload}/{dataset}/3/query_{query}.csv'
    shell:
        'python client/sage-agg/interface.py query http://localhost:{SAGE_PORT_150MS}/sparql http://localhost:{SAGE_PORT_150MS}/sparql/{wildcards.dataset} --file {input.query} --measure {output}; '


rule sage_agg_performance_format_query_files:
    input:
        ancient('output/data/sage-agg/performance/{workload}/{dataset}/{run}/query_{query}.csv')
    output:
        'output/data/sage-agg/performance/{workload}/{dataset}/{run}/mergeable_query_{query}.csv'
    shell:
        'touch {output}; '
        'echo "workload,query,execution_time,nb_calls,data_transfer" >> {output}; '
        'echo -n "{wildcards.workload},Q{wildcards.query}," >> {output}; '
        'cat {input} >> {output}; '
        'echo "" >> {output};'

rule sage_agg_performance_merge_query_files:
    input:
        expand('output/data/sage-agg/performance/{{workload}}/{{dataset}}/{{run}}/mergeable_query_{query}.csv', query=QUERIES)
    output:
        'output/data/sage-agg/performance/{workload}/{dataset}/{run}/all.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule sage_agg_performance_compute_average:
    input:
        ancient('output/data/sage-agg/performance/{workload}/{dataset}/1/all.csv'),
        ancient('output/data/sage-agg/performance/{workload}/{dataset}/2/all.csv'),
        ancient('output/data/sage-agg/performance/{workload}/{dataset}/3/all.csv')
    output:
        'output/data/sage-agg/performance/{workload}/{dataset}/all.csv'
    shell:
        'python scripts/average.py {input} {output}'


rule sage_agg_performance_format_dataset_files:
    input:
        ancient('output/data/sage-agg/performance/{workload}/{dataset}/all.csv')
    output:
        'output/data/sage-agg/performance/{workload}/{dataset}/all_mergeable.csv'
    shell:
        "awk -v d=\"{wildcards.dataset}\" -F\",\" 'BEGIN {{ OFS=\",\" }} FNR == 1 {{ $(NF+1) = \"dataset\" }} FNR > 1 {{ $(NF+1) = d; }} 1' {input} > {output}"


rule sage_agg_performance_merge_dataset_files:
    input:
        expand('output/data/sage-agg/performance/{{workload}}/{dataset}/all_mergeable.csv', dataset=BSBM_DATASETS)
    output:
        'output/data/sage-agg/performance/{workload}/data.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'


rule sage_agg_performance_merge_workload_files:
    input:
        expand('output/data/sage-agg/performance/{workload}/data.csv', workload=['SP', 'SP-ND'])
    output:
        'output/data/sage-agg/performance/all.csv'
    shell:
        'bash scripts/merge_csv.sh {input} > {output}'

####################################################################################################
########## PLOT 2: Impact of the quantum ###########################################################
####################################################################################################
