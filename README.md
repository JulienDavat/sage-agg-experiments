# Processing SPARQL Aggregate Queries with Web Preemption

**Paper submission date**: 12 December 2019

**Authors:** Arnaud Grall (GFI Informatique, LS2N), Thomas Minier (LS2N), Hala Skaf-Molli (LS2N), and Pascal Molli (LS2N)

**Abstract** 
Executing aggregate queries on the web of data allows to compute useful statistics ranging from the number of properties per class in a dataset to the average life of famous scientists per country. 
However, processing aggregate queries on public SPARQL endpoints is challeng- ing, mainly due to quotas enforcement that prevents queries to deliver complete results. 
Existing distributed query engines allow to go beyond quota limitations, but their data transfer and execution times are clearly prohibitive when processing aggregate queries. 
Following the web pre- emption model, we define a new preemptable aggregation operator that allows to suspend and resume aggregate queries.
Web preemption allows to continue query execution beyond quota limits and server-side aggre- gation drastically reduces data transfer and execution time of aggregate queries. 
Experimental results demonstrate that our approach outperforms existing approaches by several orders of magnitude in terms of execution time and the amount of transferred data.

# Experimental results

## Dataset and Queries

In our experiments, we use two workloads of 18 aggregation queries extracted from the
[SPORTAL](https://www.researchgate.net/publication/324907489_SPORTAL_Profiling_the_content_of_public_SPARQL_endpoints)
queries. The first workload, denoted [SP](https://github.com/JulienDavat/sage-sparql-void/blob/master/SP), is composed of the extracted SPORTAL
queries, while the second workload, denoted [SP-ND](https://github.com/JulienDavat/sage-sparql-void/blob/master/SP-ND), is defined by removing the 
DISTINCT modifier from the queries of SP. SP-ND is used to study the impact of
the DISTINCT modifier on the query execution performance. We run the SP
and SP-ND workloads on synthetic and real-world datasets: the Berlin SPARQL
Benchmark ([BSBM](https://www.researchgate.net/publication/220123872_The_Berlin_SPARQL_benchmark)) 
with different sizes (5k, 40k, 370k triples for BSBM-10, BSBM-100 and BSBM-1k respectively) and 
a fragment of DBpedia *v3.5.1* (150M triples) respectively.

## Machine configuration on GCP (Google Cloud Platform)

- type: `n1-standard-2 :4 vCPU, 15 Go of memory`
- processor platform: `Intel Broadwell`
- OS:  ubuntu-1910-eoan-v20191114
- SSD disk of 250GB

## Plots

**Plot 1 legend**: Data Transfer and execution time for BSBM-10, BSBM-100 and BSBM-1k, when running the SP (left) and SP-ND (right) workloads

![](output/figures/bsbm.png?raw=true)

**Plot 2 legend**: Time quantum impact executing SP (left) and SP-ND (right) on BSBM1k

![](output/figures/quantum_impacts.png?raw=true)

**Plot 3 legend**: Execution time and data transferred for SP-ND on DBpedia

![](output/figures/dbpedia.png?raw=true)

**Plot 4 legend**: Hyperloglog precision impact executing SP on BSBM1k

![](output/figures/precision_impacts.png?raw=true)

# Experimental study

## Dependencies Installation

To run our experiments, the following softwares and packages have to be installed on your system.
* [Virtuoso](https://github.com/openlink/virtuoso-opensource/releases/tag/v7.2.5) (v7.2.5)
* [PostgreSQL](https://www.postgresql.org/) (v13.1) 
* [Gradle](https://gradle.org/)
* [NodeJS](https://nodejs.org/en/) (v14.15.4)
* [Java]() (v11.0.9.1)
* [Python3.7]()
* [Miniconda](https://docs.conda.io/en/latest/miniconda.html#) (for Python3.7)

**Caution:** The default location of Virtuoso is ```/usr/local/virtuoso-opensource```. If you
change it during the installation of Virtuoso, please update the
[start_server.sh](https://github.com/JulienDavat/sage-sparql-void/blob/master/scripts/start_servers.sh) and
[stop_server.sh](https://github.com/JulienDavat/sage-sparql-void/blob/master/scripts/stop_servers.sh) scripts.

## Configuration

**Virtuoso** needs to be configured to use a single thread per query and to disable quotas.
First open the file ```${VIRTUOSO_DIRECTORY}/var/lib/virtuoso/db/virtuoso.ini``` and apply
the following changes:
- In the *SPARQL* section
    - set *ResultSetMaxRows*, *MaxQueryCostEstimationTime* and *MaxQueryExecutionTime* to **10^9** in order to disable quotas
- In the *Parameters* section
    - set *MaxQueryMem*, *NumberOfBuffers* and *MaxDirtyBuffers* to an appropriate value based on the amount of space available on your system
    - set *ThreadsPerQuery* to **1**
    - add your *home* directory to *DirsAllowed*

**PostgreSQL** needs to be configured to prepare the data loading and tuned for the web preemption
server. First open the file ```${POSTGRESQL_DIRECTORY}/main/postgresql.conf``` and apply the
following changes in the *Planner Method Configuration* section:
- Uncomment all enable_XYZ options
- Set *enable_indexscan*, *enable_indexonlyscan* and *enable_nestloop* to **on**
- Set all the other enable_XYZ options to **off**

Then, open the file ```${POSTGRESQL_DIRECTORY}/main/pg_hba.conf``` and apply the following change:
- Set the connection method for the local unix domain socket to **trust**

Finally, create a user and a database, both named aggregates
```bash 
$ sudo -u postgres psql
postgres=$ create database aggregates
postgres=$ create user aggregates with encrypted password 'aggregates'
postgres=$ grant all privileges on database aggregates to aggregates
```

## Project installation

Once all dependencies have been installed, clone this repository and install the project. 

```bash
git clone https://github.com/JulienDavat/sage-sparql-void.git sage-agg-experiments
cd sage-agg-experiments
bash install.sh
```

## Datasets ingestion

All the datasets used in our experiments are available online:
- **BSBM-10** in the [.hdt]() and [.nt]() formats
- **BSBM-100** in the [.hdt]() and [.nt]() formats
- **BSBM-1k** in the [.hdt]() and [.nt]() formats
- **DBpedia-150M** in the [.nt]() format

First, download all datasets, both the **.nt** and **.hdt** formats, into the **graphs** directory.

### Ingest data in SaGe

- Activate the project environement
```bash 
source aggregates/bin/activate
```

- Create a postgreSQL table for each dataset
```bash 
sage-postgres-init --no-index configs/sage/sage-exact-150ms.yaml bsbm10
sage-postgres-init --no-index configs/sage/sage-exact-150ms.yaml bsbm100
sage-postgres-init --no-index configs/sage/sage-exact-150ms.yaml bsbm1k
sage-postgres-init --no-index configs/sage/sage-exact-150ms.yaml subdbpedia
```

- Load each dataset into postgreSQL
```bash
sage-postgres-put graphs/bsbm10.nt configs/sage/sage-exact-150ms.yaml bsbm10
sage-postgres-put graphs/bsbm100.nt configs/sage/sage-exact-150ms.yaml bsbm100
sage-postgres-put graphs/bsbm1k.nt configs/sage/sage-exact-150ms.yaml bsbm1k
sage-postgres-put graphs/subdbpedia.nt configs/sage/sage-exact-150ms.yaml subdbpedia
```

- Create SPO, OSP and POS indexes for each postgreSQL table
```bash
sage-postgres-index configs/sage/sage-exact-150ms.yaml bsbm10
sage-postgres-index configs/sage/sage-exact-150ms.yaml bsbm100
sage-postgres-index configs/sage/sage-exact-150ms.yaml bsbm1k
sage-postgres-index configs/sage/sage-exact-150ms.yaml subdbpedia
```

### Ingest data in Virtuoso

```bash
# loading bsbm10
isql 'EXEC=DB.DBA.TTLP(file_to_string_output("${PROJECT_DIRECTORY}/graphs/bsbm10.nt"),"","http://example.org/datasets/bsbm10",0);'
# loading bsbm100
isql 'EXEC=DB.DBA.TTLP(file_to_string_output("${PROJECT_DIRECTORY}/graphs/bsbm100.nt"),"","http://example.org/datasets/bsbm100",0);'
# loading bsbm1k
isql 'EXEC=DB.DBA.TTLP(file_to_string_output("${PROJECT_DIRECTORY}/graphs/bsbm1k.nt"),"","http://example.org/datasets/bsbm1k",0);'
# loading dbpedia150M
isql 'EXEC=DB.DBA.TTLP(file_to_string_output("${PROJECT_DIRECTORY}/graphs/subdbpedia.nt"),"","http://example.org/datasets/subdbpedia",0);'
```

## Experiments configuration

All our experiments can be configured using the file [xp.json](https://github.com/JulienDavat/sage-sparql-void/blob/master/configs/xp.json).

```json
"settings": {
    "plot1" : {
        "title": "Data transfer and execution time for BSBM-10, BSBM-100 and BSBM-1k, when running the SP and SP-ND workload",
        "settings": {
            "datasets": ["bsbm10", "bsbm100", "bsbm1k"],
            "approaches": ["sage", "sage-agg", "sage-approx", "virtuoso", "comunica"],
            "workloads": ["SP", "SP-ND"],
            "queries": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            "warmup": true,
            "runs": 3
        }
    },
    "plot2": {
        "title": "Time quantum impacts executing SP and SP-ND over BSBM-1k",
        "settings": {
            "quantums": ["150", "1500", "15000"],
            "approaches": ["sage", "sage-agg", "sage-approx", "virtuoso"],
            "workloads": ["SP", "SP-ND"],
            "queries": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            "runs": 3,
            "warmup": true
        }
    },
    "plot3": {
        "title": "Data transfer and execution time for dbpedia, when running the SP and SP-ND workload",
        "settings": {
            "datasets": ["subdbpedia"],
            "approaches": ["sage-agg", "sage-approx", "virtuoso"],
            "workloads": ["SP", "SP-ND"],
            "queries": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            "runs": 1,
            "warmup": true
        }
    },
    "plot4": {
        "title": "Hyperloglog precision impacts executing SP and SP-ND over BSBM-1k",
        "settings": {
            "precisions": ["98", "95", "90"],
            "approaches": ["sage", "sage-agg", "sage-approx", "virtuoso"],
            "workloads": ["SP"],
            "queries": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            "runs": 3,
            "warmup": true
        }
    }
}
```

Configurable fields are detailed below:
- **datasets:** the datasets on which queries will be executed. 
    - accepted values: bsbm10, bsbm100, bsbm1k and subdbpedia
- **quantums:** the different quantum values for which queries will be executed. 
    - accepted values: 150, 1500 and 15000
- **precisions:** the different precisions for which queries will be executed. 
    - accepted values: 90, 95 and 98 
- **workloads:** the workloads to execute. 
    - accepted values: SP and SP-ND
- **queries:** the queries to execute for each workload. 
    - accepted values: 1..18
- **warmup:** true to compute a first run for which no statistics will be recorded, false otherwise
    - accepted value: true or false
- **runs:** the number of run to compute. For each of these run, queries execution statistics will be
recorded and the average will be retained for each metric.
    - accepted value: a non-zero positive integer

## Running the experiments 

Our experimental study is powered by **Snakemake**. To run any part of our experiments, just ask snakemake for the desired output file.
For example, the main commands are given below:

```bash
snakemake --cores 1 output/figures/bsbm.png # ask for the plot of the first experiment

snakemake --cores 1 output/figures/quantum_impacts.png # ask for the plot of the second experiment

snakemake --cores 1 output/figures/dbpedia.png # ask for the plot of the third experiment

snakemake --cores 1 output/figures/{bsbm,quantum_impacts,dbpedia,precision_impacts}.png # ask for all plots
```

It is also possible to run any part of the experiments without Snakemake. For example, the important commands are given below:

```bash
# starts the SaGe server on the port 8080 with a time quantum of 150ms, approximations disable and 1 worker
sage configs/sage/sage-exact-150ms.yaml -w 1 -p 8080

# starts the LDF server on the port 8000
node_modules/ldf-server/bin/ldf-server configs/ldf/config.json 8000 1

# starts the Virtuoso server on the port 8890
${VIRTUOSO_DIRECTORY}/bin/virtuoso-t -f -c ${VIRTUOSO_DIRECTORY}/var/lib/virtuoso/db/virtuoso.ini

# evaluates the first query of the SP workload on the bsbm1k dataset with the SaGe client
java -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:8080/sparql/bsbm1k --file queries/SP/query_1.sparql

# evaluates the first query of the SP workload on the bsbm1k dataset with the SaGe-Agg client
python client/sage-agg/interface.py query http://localhost:8080/sparql http://localhost:8080/sparql/bsbm1k --file queries/SP/query_1.sparql --display

# evaluates the first query of the SP workload on the bsbm1k dataset with Virtuoso
python client/virtuoso/interface.py query http://localhost:8890/sparql http://example.org/datasets/bsbm1k --file queries/SP/query_1.sparql --display

# evalutes the first query of the SP workload on the bsbm1k dataset with Comunica
node client/comunica/interface.js http://localhost:8080/sparql/bsbm1k --file queries/SP/query_1.sparql --display
```