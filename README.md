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

**Plot 1 legend**: Data Transfer and execution time for BSBM-10, BSBM-100 and BSBM- 1k, when running the SP (left) and SP-ND (right) workloads

![](plot-1.png?raw=true)

**Plot 2 legend**: Time quantum impact executing SP (left) and SP-ND (right) on BSBM1k

![](plot-2.png?raw=true)

**Plot 3 legend**: Execution time and data transferred for SP-ND on DBpedia

![](plot-3.png?raw=true)

## Repository

**Client** can be installed using `java` and `gradle`
Experiments are made using java 11 and installation the last version of gradle.
Once repository has been cloned.
```bash
gradle fatJar # generate a full jar into build/lib/
java -jar build/libs/sage-sparql-void-fat-1.0.jar --help
```

**Server** can be installed using `python3.7`, `pip` and `virtualenv` (highly recommended)
```
python3 -m pip install virtualenv
virtualenv env
source env/bin/activate
cd server/sage-agg
pip install -r requirements.txt
pip install pybind11
pip install -e .[hdt,postgres]
```
Now you can run the server using the usual command: `sage -w 1 -p 7120 [config_file.yaml]`; run for help `sage --help`

### Enabling sage aggregate queries

Once a server is running on port 7120 on localhost for example:
```bash
# for using the sage normal
java -jar -jar build/libs/sage-sparql-void-fat-1.0.jar query 
    http://localhost:7120/sparql/[yourdatasetname] 
    --query "select (count(*) as ?count) where {?s ?p ?o}"
# for using the sage with aggregate queries, add 2 new options
java -jar -jar build/libs/sage-sparql-void-fat-1.0.jar query 
    http://localhost:7120/sparql/[yourdatasetname] 
    --query "select (count(*) as ?count) where {?s ?p ?o}" --optimized --buffer 0
```

### Additional features

You can also query a sparql endpoint using the same jar.
```bash
java -jar -jar build/libs/sage-sparql-void-fat-1.0.jar sparql-endpoint 
    [--format=<format>] <endpoint> <query> <default_graph>
```

**Please pay attention** that other commands such as `sportal-sparql-endpoint`, `dataset` and `endpoint` could not work as they were not tested for a while

## Dockerfiles

We provide a docker-compose file for fast loading of each components, sage-agg, tpf server, virtuoso and the postgresql database.
```bash
# if not install docker-compose first
docker-compose -f experiments/compose.yaml up -d sage-virtuoso # for virtuoso server
docker-compose -f experiments/compose.yaml up -d sage-postgres # for sage-postgres server
docker-compose -f experiments/compose.yaml up -d sage-agg # for sage-agg server
docker-compose -f experiments/compose.yaml up -d sage-tpf # for tpf server
```  
see the experiments/compose.yaml for more details.

## Machine configuration on GCP (Google Cloud Platform)

We run experiments in a 
- type: `n1-standard-2 :2 vCPU, 7.5 Go of memory`
- processor platform: `Intel Broadwell`
- OS:  ubuntu-1910-eoan-v20191114
- SSD disk of 50gb on the main instance
- SSD local disk of 375gb (All data were stored on this local disk.)
- total approximative cost per month: 108$

Thus we installed:
- python3.7 (`Python 3.7.5`) last version
- gradle (`Gradle 6.0.1`)
- postgres (`psql (PostgreSQL) 11.5 (Ubuntu 11.5-1)`)
- java (`openjdk 11.0.5-ea 2019-10-15`)
- docker (`Docker version 19.03.3, build a872fc2f86`)
- docker-compose (`docker-compose version 1.25.0, build 0a186604`)

For experiments the postgresql and the sage-agg server where loaded manually (not using the docker-compose).
The virtuoso server and tpf server where loaded using the docker-compose file.


