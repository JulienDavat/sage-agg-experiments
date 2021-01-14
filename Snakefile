# QUERIES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
# QUERIES = [1, 2, 3]
# RUNS = [1, 2, 3]

configfile: "configs/xp.json"

####################################################################################################
########## INITIALIZATION ##########################################################################
####################################################################################################

onstart:
    shell('mkdir -p output/log')
    shell('bash scripts/start_servers.sh')
    print('Starting all servers...')

onsuccess:
    shell('bash scripts/stop_servers.sh')
    print('Shutting down all servers')

onerror:
    shell('bash scripts/stop_servers.sh')
    print('Shutting down all servers')

####################################################################################################
########## DEPENDENCIES ############################################################################
####################################################################################################

def last_run(plot):
    runs = config["settings"][f"plot{plot}"]["settings"]["runs"]
    if config["settings"][f"plot{plot}"]["settings"]["warmup"]:
        return runs + 1
    else:
        return runs

def first_run(plot):
    if config["settings"][f"plot{plot}"]["settings"]["warmup"]:
        return 2
    else:
        return 1

include: 'rules/plot1.smk'
include: 'rules/plot2.smk'
include: 'rules/plot3.smk'