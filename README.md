# sage-sparql-void

Execution of VoID description over Sage endpoints

## Install

Using gradle distZip
```bash
git clone https://github.com/folkvir/sage-sparql-void.git
cd sage-sparql-void
gradle clean distZip
cd build/distributions/
unzip sage-sparql-void.zip
cd sage-sparql-void
./bin/sage-sparql-void
```
Using gradle fatJar
```bash
git clone https://github.com/folkvir/sage-sparql-void.git
cd sage-sparql-void
gradle clean fatjar
java -jar build/libs/sage-sparql-void-fat-1.0.jar
```

## Running the Sage server for the experiment

**Due to proxy settings: change any proxy values inside the Dockerfile or the gradle command to fit your settings.**

```bash
# build the project
gradle -Dhttp.proxyHost=cache.ha.univ-nantes.fr -Dhttp.proxyPort=3128 -Dhttps.proxyHost=cache.ha.univ-nantes.fr -Dhttps.proxyPort=3128 clean build fatJar
```

Run sage server using a custom GUnicorn configuration
```bash
#
cd sage-engine
git submodule init
git submodule update
cd ..
```
Then:
```bash
cd experiments/
docker-compose up -d
```

Then for running the experiments:

```bash
# 2017 (2B)
nohup bash ./start_wikidata2b.bash &
# 2018 (8B)
nohup java -Xms50g -Xmx50g -jar build/libs/sage-sparql-void-fat-1.0.jar dataset http://172.16.8.50:7180/sparql/wikidata http://172.16.8.50:7180/sparql/wikidata ./output/ &
```
