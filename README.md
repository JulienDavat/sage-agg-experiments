# Dealing with aggregation using Sage

## Installation
```bash
git clone https://github.com/sage-sparql-void
cd sage-sparql-void
```
### Client
Requirements:
- python 3.7+ with virtualenv and matplotlib and hdt
- Java 8+
- Libraries: 
    - gradle


```bash
gradle clean fatJar
java -jar build/libs/sage sage-sparql-void-fat-1.0.jar --help
```

### Server

Requirements: 
- Python 3.7, virtualenv
- Libraries: [hdt](http://github.com/folkvir/pyHDT), postgres

Please use the Dockerfile (`experiments/sage/Dockerfile`) to build the image
It will download and build hdt and rocksdb.

## Enable server aggregations to be processed on the server
```bash
# just passed to the client the --optimized option
java -jar build/libs/sage sage-sparql-void-fat-1.0.jar query <...> --optimized --buffer <buffer_size_in_bytes>
```

## Memento

````bash
nohup bash experiments/scripts/run-virtuoso.sh 3 output/virtuoso "bsbm10,bsbm100,bsbm1k"  &
nohup bash experiments/scripts/run-tpf.sh 3 output/tpf "bsbm10,bsbm100,bsbm1k"  &
nohup bash experiments/scripts/run-tpf.sh 3 output/sage "bsbm10,bsbm100,bsbm1k" "0,100000,1000000,1000000000" &
# dbpedia
nohup bash experiments/scripts/run-sage.sh 3 output/sage-dbpedia "dbpedia351" "0,100000,1000000,1000000000" &

# big 
nohup bash experiments/scripts/run-virtuoso.sh 3 output/third/virtuoso/ "bsbm10,bsbm100,bsbm1k" localhost:7130 data/queries/to_run.txt && \
    bash experiments/scripts/run-tpf.sh 3 output/third/tpf/ "bsbm10,bsbm100,bsbm1k" localhost:7140 data/queries/to_run.txt && \
    bash experiments/scripts/run-sage.sh 3 output/third/sage/ "bsbm10,bsbm100,bsbm1k" "0,100000,1000000,1000000000" localhost:7120 data/queries/to_run.txt &
    
     

````


