# Dealing with aggregation using Sage

## Installation
```bash
git clone https://github.com/sage-sparql-void
cd sage-sparql-void
```
### Client
Requirements:
- Java 8
- Libraries: gradle

```bash
gradle clean fatJar
java -jar build/libs/sage sage-sparql-void-fat-1.0.jar --help
```

### Server

Requirements: 
- Python 3.7
- Libraries: rocksdb, [hdt](http://github.com/folkvir/pyHDT), postgres

Please use the Dockerfile (`experiments/sage/Dockerfile`) to build the image
It will download and build hdt and rocksdb.

## Enable server aggregations to be processed on disk
```bash
# just passed to the client the --optimized option
java -jar build/libs/sage sage-sparql-void-fat-1.0.jar query <...> --optimized

```