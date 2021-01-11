# QUERIES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
QUERIES = [1, 2, 3]
RUNS = [1, 2, 3]

####################################################################################################
########## INITIALIZATION ##########################################################################
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
########## DEPENDENCIES ############################################################################
####################################################################################################

include: 'rules/plot1.smk'
include: 'rules/plot2.smk'
include: 'rules/plot3.smk'