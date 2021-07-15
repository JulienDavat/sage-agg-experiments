# Processing SPARQL Aggregate Queries with Web Preemption

**Authors:** Julien Aimonier-Davat (LS2N), Hala Skaf-Molli (LS2N), and Pascal Molli (LS2N), Arnaud Grall (GFI Informatique, LS2N), Thomas Minier (LS2N)

**Abstract**
Getting complete results when processing aggregate queries on public SPARQL endpoints is challenging, mainly due
to quota enforcement. Although Web preemption allows to process aggregate queries online, on preemptable SPARQL
servers, data transfer is still very large when processing count-distinct aggregate queries. In this paper, it is shown that count-distinct aggregate queries can be approximated with low data transfer by extending the partial aggregations operator with HyperLogLog++ sketches. Experimental results demonstrate that the proposed approach outperforms existing approaches by orders of
magnitude in terms of data transfer.

> This paper is an extension of the ESWC2020 article: [Processing SPARQL Aggregate Queries with Web Preemption](https://hal.archives-ouvertes.fr/hal-02511819/document). The experimental results of the first paper are available [here](https://github.com/folkvir/sage-sparql-void).

# Experimental results

## Dataset and Queries

In our experiments, we use two workloads of 18 SPARQL aggregation queries extracted from the
[SPORTAL](https://www.researchgate.net/publication/324907489_SPORTAL_Profiling_the_content_of_public_SPARQL_endpoints)
queries. The first workload, denoted [SP](https://github.com/JulienDavat/sage-sparql-void/blob/master/SP), is composed
of 18 SPORTAL queries where *CONSTRUCT* and *FILTER* clauses have been removed. The second workload,
denoted [SP-ND](https://github.com/JulienDavat/sage-sparql-void/blob/master/SP-ND), is defined by removing the
*DISTINCT* modifier from the queries of SP. SP-ND is used to study the impact of
the *DISTINCT* modifier on the query execution performance. We run the SP
and SP-ND workloads on synthetic and real-world datasets: the Berlin SPARQL
Benchmark ([BSBM](https://www.researchgate.net/publication/220123872_The_Berlin_SPARQL_benchmark))
with different sizes (5k, 40k, 370k triples for BSBM-10, BSBM-100 and BSBM-1k respectively) and
a fragment of DBpedia *v3.5.1* (100M triples) respectively.


## Compared Approaches

We compare the following approaches:
- **SaGe** is the SaGe query engine as defined in the [web preemption](https://hal.archives-ouvertes.fr/hal-02017155/document) model.
The SaGe server is implemented in Python and the code is available [here](https://github.com/JulienDavat/sage-sparql-void/tree/master/server/sage-agg). The SaGe smart client is implemented as an extension
of Apache Jena. The code is available [here](https://github.com/JulienDavat/sage-sparql-void/tree/master/client/sage). In our experiments, the SaGe server is configured with a maximum page size of **10MB**.
Datasets are stored in an SQLite database with B-Tree indexes on *SPO*, *OSP* and *POS*.

- **SaGe-agg** is an extension of SaGe with our new partial aggregations operator. The server-side algorithms (Algorithms 1, 2 and 3 in the paper)
are implemented on the SaGe server. The code is available [here](https://github.com/JulienDavat/sage-sparql-void/tree/master/server/sage-agg/sage/query_engine/iterators/aggregates). The client-side algorithms (Algorithms 3 and 4 in the paper) are
implemented in a Python client. The code is available [here](https://github.com/JulienDavat/sage-sparql-void/tree/master/client/sage-agg). For a fair comparison, SaGe-agg runs against the same server as SaGe.

- **SaGe-approx** is the same extension as SaGe-agg where *COUNT DISTINCT* queries are evaluated using a probabilistic count algorithm
([Hyperloglog++](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/40671.pdf)). When SaGe-approx is used, the SaGe server is configured to compute
*COUNT DISTINCT* queries with an error rate of **2%**.

- **TPF** is the TPF query engine as defined in the [Triple Pattern Fragments](https://www.sciencedirect.com/science/article/pii/S1570826816000214?casa_token=zWODejXE39sAAAAA:RPCFYKEWtqJWpO2XF-zr55ze7M46PxRldPDt-NDwVgKZ2bIiGhJa2UEOXCCkgrPMYA6I6KZ9TKA) model. We use the
[Linked Data Fragments Server](https://www.npmjs.com/package/ldf-server) as the TPF server and [Comunica](https://github.com/comunica/comunica)
as the TPF smart client. The server is configured without Web cache and with a page size of **10000** triples. The data are stored
using the HDT format.

- **Virtuoso** is the Virtuoso SPARQL endpoint (v7.2.4). Virtuoso is configured **without quotas** in order to deliver complete results. We also configured Virtuoso with a *single thread* per query to fairly compare with other engines.


## Evaluation Metrics
- **Execution time** is the total time between starting the query execution and
the production of the final results by the client.
- **Data transfer** is the total number of bytes transferred from the server to
the client during the query execution.
- **Error rate** is defined as the difference between the real cardinality *c* and the estimated cardinality *c'* : (1 - (min(*c*, *c'*) / max(*c*, *c'*)) x 100


## Machine configuration on GCP (Google Cloud Platform)

- type: `n2-highmem-4 :4 vCPU, 32 Go of memory`
- processor platform: `Intel Broadwell`
- OS:  `ubuntu-1910-eoan-v20191114`
- Disk: `Local SSD disk of 375GB`


## Plots

**Plot 1:** Data Transfer and execution time for BSBM-10, BSBM-100 and BSBM-1k, when running the SP (left) and SP-ND (right) workloads

![](output/figures/bsbm.png?raw=true)

**Plot 2:** Impact of the quantum when running SP (left) and SP-ND (right) workloads on BSBM1k

![](output/figures/quantum_impacts.png?raw=true)

**Plot 3:** Execution time for each query of the SP workload with an infinite quantum on BSBM1k

![](output/figures/SP_bsbm1k_execution_times.png?raw=true)

**Plot 4:** Data transfer and execution time when running the SP workload on DBpedia

![](output/figures/dbpedia_performance.png?raw=true)

**Plot 5** Impact of the Hyperloglog++ precision when running the SP workload on BSBM1k

![](output/figures/precision_impacts.png?raw=true)

**Plot 6** Average error rate for the *GroupKeys* of the SP workload queries on DBpedia according to the number of distinct values

![](output/figures/error_rates.png?raw=true)


# Steps to reproduce the results and figures

## Dependencies Installation

To run our experiments, the following softwares and packages have to be installed on your system.
* [Virtuoso](https://github.com/openlink/virtuoso-opensource/releases/tag/v7.2.5) (v7.2.5)
* [Gradle](https://gradle.org/)
* [NodeJS](https://nodejs.org/en/) (v14.15.4)
* [Java](https://www.java.com/fr/) (v11.0.9.1)
* [Python3.7](https://www.python.org) and [Python3-dev]()

**Caution:** In the next sections, we assume that Virtuoso is installed at its default location
```/usr/local/virtuoso-opensource```. If you change the location of Virtuoso, do not forget to
propagate this change in the next instructions and please update the
[start_server.sh](https://github.com/JulienDavat/sage-sparql-void/blob/master/scripts/start_servers.sh) and
[stop_server.sh](https://github.com/JulienDavat/sage-sparql-void/blob/master/scripts/stop_servers.sh) scripts.

## Configuration

**Virtuoso** needs to be configured to use a single thread per query and to disable quotas.
- In the *\[SPARQL\]* section of the file ```/usr/local/virtuoso-opensource/var/lib/virtuoso/db/virtuoso.ini```
    - set *ResultSetMaxRows*, *MaxQueryCostEstimationTime* and *MaxQueryExecutionTime* to **10^9** in order to disable quotas.
- In the *\[Parameters\]* section of the file ```/usr/local/virtuoso-opensource/var/lib/virtuoso/db/virtuoso.ini```
    - set *MaxQueryMem*, *NumberOfBuffers* and *MaxDirtyBuffers* to an appropriate value based on the
    amount of RAM available on your machine. In our experiments, we used the settings recommended for
    a machine with 16Go of RAM.
    - set *ThreadsPerQuery* to **1**.
    - add your *home* directory to *DirsAllowed*.


## Project installation

Once all dependencies have been installed, clone this repository and install the project.

```bash
git clone https://github.com/JulienDavat/sage-sparql-void.git sage-agg-experiments
cd sage-agg-experiments
bash install.sh
```

## Datasets ingestion

All datasets used in our experiments are available online. Download all datasets,
both the **.hdt** and the **.nt** formats, into the **graphs** directory.

```bash
# Move into the graphs directory
cd graphs

# Downloads the BSBM-10 dataset
wget nas.jadserver.fr/thesis/projects/aggregates/datasets/bsbm10.hdt
wget nas.jadserver.fr/thesis/projects/aggregates/datasets/bsbm10.nt

# Downloads the BSBM-100 dataset
wget nas.jadserver.fr/thesis/projects/aggregates/datasets/bsbm100.hdt
wget nas.jadserver.fr/thesis/projects/aggregates/datasets/bsbm100.nt

# Downloads the BSBM-1K dataset
wget nas.jadserver.fr/thesis/projects/aggregates/datasets/bsbm1k.hdt
wget nas.jadserver.fr/thesis/projects/aggregates/datasets/bsbm1k.nt

# Downloads the DBpedia100M dataset. We do not need the .hdt as TPF is not tested on the DBpedia100M dataset.
wget nas.jadserver.fr/thesis/projects/aggregates/datasets/dbpedia100M.nt
```

### Ingest data in SaGe

- Activate the project environement
```bash
source aggregates/bin/activate
```

- Create a table for each dataset
```bash
sage-sqlite-init --no-index configs/sage/sage-exact-150ms.yaml bsbm10
sage-sqlite-init --no-index configs/sage/sage-exact-150ms.yaml bsbm100
sage-sqlite-init --no-index configs/sage/sage-exact-150ms.yaml bsbm1k
sage-sqlite-init --no-index configs/sage/sage-exact-150ms.yaml dbpedia100M
```

- Load each dataset into the database
```bash
sage-sqlite-put graphs/bsbm10.nt configs/sage/sage-exact-150ms.yaml bsbm10
sage-sqlite-put graphs/bsbm100.nt configs/sage/sage-exact-150ms.yaml bsbm100
sage-sqlite-put graphs/bsbm1k.nt configs/sage/sage-exact-150ms.yaml bsbm1k
sage-sqlite-put graphs/dbpedia100M.nt configs/sage/sage-exact-150ms.yaml dbpedia100M
```

- Create SPO, OSP and POS indexes for each table
```bash
sage-sqlite-index configs/sage/sage-exact-150ms.yaml bsbm10
sage-sqlite-index configs/sage/sage-exact-150ms.yaml bsbm100
sage-sqlite-index configs/sage/sage-exact-150ms.yaml bsbm1k
sage-sqlite-index configs/sage/sage-exact-150ms.yaml dbpedia100M
```

### Ingest data in Virtuoso

```bash
# Loads all .nt files stored in the graphs directory
/usr/local/virtuoso-opensource/bin/isql "EXEC=ld_dir('{PROJECT_DIRECTORY}/graphs', '*.nt', 'http://example.org/datasets/default');"
/usr/local/virtuoso-opensource/bin/isql "EXEC=rdf_loader_run();"
/usr/local/virtuoso-opensource/bin/isql "EXEC=checkpoint;"
```

## Servers configuration

### SaGe server configuration

The SaGe server can be configured using a *.yaml* file. The different configurations used in our experiments are available in the directory
[configs/sage](https://github.com/JulienDavat/sage-sparql-void/tree/master/configs/sage). An example of a valid configuration file is detailed below.

```yaml
quota: 150 # the time (in milliseconds) of a quantum.
max_results: 10000 # the maximum number of mappings that can be returned per quantum.
max_group_by_size: 10485760 # the maximum space (in bytes) allocated to the partial aggregation operator. Replaces 'max_results' for aggregation queries.
enable_approximation: false # true to compute COUNT DISTINCT queries with a probabilistic count algorithm, false otherwise.
error_rate: 0.02 # the error rate for the probabilistic count algorithm.
datasets: # the datasets that can be queried.
- name: bsbm10 # the name of the dataset. Used to select the dataset we want to query.
  backend: sqlite # the backend used to store the dataset.
  database: graphs/sage_sqlite.db # the location of the database file.
- name: bsbm100
  backend: sqlite
  database: graphs/sage_sqlite.db
- name: bsbm1k
  backend: sqlite
  database: graphs/sage_sqlite.db
- name: dbpedia100M
  backend: sqlite
  database: graphs/sage_sqlite.db
```

### LDF server configuration

The LDF server can be configured using a *.json* file. The different configurations used in our experiments are available in the directory
[configs/ldf](https://github.com/JulienDavat/sage-sparql-void/tree/master/configs/ldf). An example of a valid configuration file is detailed below.

```json
{
    "pageSize":10000, // the maximum number of mappings that can be returned per triple pattern query.
    "datasources": { // the datasets that can be queried.
        "bsbm10": { // the name of the dataset. Used to select the dataset we want to query.
            "type": "HdtDatasource", // the backend used to store the dataset.
            "settings": {
                "file": "graphs/bsbm10.hdt" // the location of the HDT file.
            }
        },
        "bsbm100": {
            "type": "HdtDatasource",
            "settings": { "file": "graphs/bsbm100.hdt" }
        },
        "bsbm1k": {
            "type": "HdtDatasource",
            "settings": { "file": "graphs/bsbm1k.hdt" }
        }
    }
}
```

### Virtuoso configuration

The configuration of Virtuoso is explained in the *Configuration* section.

## Experimental study configuration

All our experiments can be configured using the file [xp.json](https://github.com/JulienDavat/sage-sparql-void/blob/master/configs/xp.json).

```json
{
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
                "quantums": ["75", "150", "1500", "15000"],
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
                "datasets": ["dbpedia100M"],
                "approaches": ["sage-agg", "sage-approx", "virtuoso"],
                "workloads": ["SP", "SP-ND"],
                "queries": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
                "runs": 3,
                "warmup": false
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
}
```

Configurable fields are detailed below:
- **datasets:** the datasets on which queries will be executed.
    - accepted values: bsbm10, bsbm100, bsbm1k and dbpedia100M
- **quantums:** the different quantum values for which queries will be executed.
    - accepted values: 75, 150, 1500 and 15000
- **precisions:** the different precisions for which queries will be executed.
    - accepted values: 90, 95 and 98
- **workloads:** the workloads to execute.
    - accepted values: SP and SP-ND
- **queries:** the queries to execute for each workload.
    - accepted values: 1..18
- **warmup:** true to compute a first run for which no statistics will be recorded, false otherwise
    - accepted value: true or false
- **runs:** the number of run to compute. For each of these run, queries execution statistics will be recorded and the average will be retained for each metric.
    - accepted value: a non-zero positive integer

## Running the experiments

Our experimental study is powered by **Snakemake**. To run any part of our experiments, just ask snakemake for the desired output file.
For example, the main commands are given below:

```bash
# Measures the performance in terms of execution time and data transfer for different graph sizes, for the SP and SP-ND workloads
snakemake --cores 1 output/figures/bsbm_performance.png

# Measures the impact of the quantum in terms of execution time and data transfer, for the SP and SP-ND workloads, on the BSBM-1k dataset
snakemake --cores 1 output/figures/quantum_impacts.png

# Measures the performance in terms of execution time and data transfer, for the SP workload, on the DBpedia100M dataset
snakemake --cores 1 output/figures/dbpedia_performance.png

# Measures the impact of the precision in terms of execution time and data transfer, for the SP workload, on the BSBM-1k dataset
snakemake --cores 1 output/figures/precision_impacts.png

# Runs all experiments
snakemake --cores 1 output/figures/{bsbm_performance,quantum_impacts,dbpedia,precision_impacts}.png
```

It is also possible to run each part of our experiments without Snakemake. For example, some important commands are given below:

```bash
# to start the SaGe server
sage configs/sage/$CONFIG_FILE -w $NB_WORKERS -p $PORT

# to start the LDF server
node_modules/ldf-server/bin/ldf-server configs/ldf/$CONFIG_FILE $PORT $NB_WORKERS

# to start the Virtuoso server on the port 8890
/usr/local/virtuoso-opensource/bin/virtuoso-t -f -c /usr/local/virtuoso-opensource/var/lib/virtuoso/db/virtuoso.ini

# to evaluate a query with SaGe-agg/approx (depending on the server configuration)
python client/sage-agg/interface.py query http://localhost:$PORT/sparql http://localhost:$PORT/sparql/$DATASET_NAME \
    --file $QUERY_FILE \
    --measure $OUT_STATS \
    --output $OUT_RESULT

# to evaluate a query with SaGe
java -jar client/sage/build/libs/sage-jena-fat-1.0.jar query http://localhost:$PORT/sparql/$DATASET_NAME \
    --file $QUERY_FILE \
    --measure $OUT_STATS \
    --output $OUT_RESULT

# to evaluate a query with Virtuoso
python client/virtuoso/interface.py query http://localhost:8890/sparql http://example.org/datasets/$DATASET_NAME \
    --file $QUERY_FILE \
    --measure $OUT_STATS \
    --output $OUT_RESULT

# to evaluate a query with Comunica
node client/comunica/interface.js http://localhost:$PORT/$DATASET_NAME \
    --file $QUERY_FILE \
    --measure $OUT_STATS \
    --output $OUT_RESULT
```
